/**
 * Copyright 2009 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.log4j.Level;
import org.junit.Test;

public class TestIOFencing {
  static final Log LOG = LogFactory.getLog(TestIOFencing.class);
  static {
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
  }

  public static class CompactionBlockerRegion extends HRegion {
    boolean compactionsBlocked = false;
    Object compactionsBlockedLock = new Object();

    Object compactionWaitingLock = new Object();
    boolean compactionWaiting = false;
    
    volatile int compactCount = 0;

    public CompactionBlockerRegion(Path basedir, HLog log, FileSystem fs, Configuration conf,
        HRegionInfo regionInfo, FlushRequester flushListener) {
      super(basedir, log, fs, conf, regionInfo, flushListener);
    }
    
    public void stopCompactions() {
      synchronized (compactionsBlockedLock) {
        compactionsBlocked = true;
      }
    }
    
    public void allowCompactions() {
      synchronized (compactionsBlockedLock) {
        compactionsBlocked = false;
        compactionsBlockedLock.notifyAll();
      }
    }
    
    public void waitForCompactionToBlock() throws InterruptedException {
      synchronized (compactionWaitingLock) {
        while (!compactionWaiting) {
          compactionWaitingLock.wait();
        }
      }
    }
    
    @Override
    protected void doRegionCompactionPrep() throws IOException {
      synchronized (compactionWaitingLock) {
        compactionWaiting = true;
        compactionWaitingLock.notifyAll();
      }
      synchronized (compactionsBlockedLock) {
        while (compactionsBlocked) {
          try {
            compactionsBlockedLock.wait();
          } catch (InterruptedException e) {
            throw new IOException(e);
          }
        }
      }
      synchronized (compactionWaitingLock) {
        compactionWaiting = false;
        compactionWaitingLock.notifyAll();
      }
      super.doRegionCompactionPrep();
    }
    
    @Override
    public byte [] compactStores() throws IOException {
      try {
        return super.compactStores();
      } finally {
        compactCount++;
      }
    }
    
    public int countStoreFiles() {
      int count = 0;
      for (Store store : stores.values()) {
        count += store.getNumberOfstorefiles();
      }
      return count;
    }
  }

  private final static HBaseTestingUtility
      TEST_UTIL = new HBaseTestingUtility();

  private final static byte[] TABLE_NAME = Bytes.toBytes("tabletest");
  private final static byte[] FAMILY = Bytes.toBytes("family");
  
  private static final int FIRST_BATCH_COUNT = 4000;
  private static final int SECOND_BATCH_COUNT = 4000;  
  
  @Test
  public void testFencingAroundCompaction() throws Exception {
    Configuration c = TEST_UTIL.getConfiguration();
    c.setClass(HConstants.REGION_IMPL,
        CompactionBlockerRegion.class,
        HRegion.class);
    c.setBoolean("dfs.support.append", true);
    // encourage plenty of flushes
    c.setLong("hbase.hregion.memstore.flush.size", 200000);
    // Only run compaction when we tell it to
    c.setInt("hbase.hstore.compactionThreshold", 1000);
    c.setLong("hbase.hstore.blockingStoreFiles", 1000);
    // Compact quickly after we tell it to!
    c.setInt("hbase.regionserver.thread.splitcompactcheckfrequency", 1000);

    LOG.info("Starting mini cluster");
    TEST_UTIL.startMiniCluster(1);
    CompactionBlockerRegion compactingRegion = null;
    
    try {
      LOG.info("Creating admin");
      HBaseAdmin admin = new HBaseAdmin(c);
      LOG.info("Creating table");
      TEST_UTIL.createTable(TABLE_NAME, FAMILY);
      HTable table = new HTable(TABLE_NAME);

      LOG.info("Loading test table");
      // Load some rows
      TEST_UTIL.loadNumericRows(table, FAMILY, 0, FIRST_BATCH_COUNT);
      
      // Find the region
      
      List<HRegion> testRegions = 
        TEST_UTIL.getMiniHBaseCluster().findRegionsForTable(TABLE_NAME);
      assertEquals(1, testRegions.size());
      compactingRegion = (CompactionBlockerRegion)testRegions.get(0);
      assertTrue(compactingRegion.countStoreFiles() > 1);
      byte REGION_NAME[] = compactingRegion.getRegionName();
      
      LOG.info("Blocking compactions");
      compactingRegion.stopCompactions();

      LOG.info("Asking for compaction");
      admin.majorCompact(TABLE_NAME);
      
      LOG.info("Waiting for compaction to be about to start");
      compactingRegion.waitForCompactionToBlock();

      LOG.info("Starting a new server");
      RegionServerThread newServerThread =
        TEST_UTIL.getMiniHBaseCluster().startRegionServer();
      HRegionServer newServer = newServerThread.getRegionServer();
      
      LOG.info("Killing region server ZK lease");
      TEST_UTIL.expireRegionServerSession(0);
      

      CompactionBlockerRegion newRegion = null;
      long startWaitTime = System.currentTimeMillis();
      while (newRegion == null) {
        LOG.info("Waiting for the new server to pick up the region");
        Thread.sleep(1000);
        newRegion =
          (CompactionBlockerRegion)newServer.getOnlineRegion(REGION_NAME);
        assertTrue("Timed out waiting for new server to open region",
            System.currentTimeMillis() - startWaitTime < 60000);
      }
      
      LOG.info("Allowing compaction to proceed");
      compactingRegion.allowCompactions();
      
      while (compactingRegion.compactCount == 0) {
        Thread.sleep(1000);
      }
      
      LOG.info("Compaction finished, loading more data");
      
      // Now we make sure that the region isn't totally confused
      TEST_UTIL.loadNumericRows(
          table, FAMILY, FIRST_BATCH_COUNT, FIRST_BATCH_COUNT + SECOND_BATCH_COUNT);
            
      admin.majorCompact(TABLE_NAME);
      startWaitTime = System.currentTimeMillis();
      while (newRegion.compactCount == 0) {
        Thread.sleep(1000);
        assertTrue("New region never compacted",
            System.currentTimeMillis() - startWaitTime < 30000);
      }
      
      assertEquals(FIRST_BATCH_COUNT + SECOND_BATCH_COUNT,
          TEST_UTIL.countRows(table));
    } finally {
      if (compactingRegion != null) {
        compactingRegion.allowCompactions();
      }
      TEST_UTIL.shutdownMiniCluster();
    }
  }

}
