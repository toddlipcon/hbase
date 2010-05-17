/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.FaultInjector;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;


@SuppressWarnings("deprecation")
public class TestCompactionCompletionDuringRecovery extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestCompaction.class.getName());
  
  private MiniDFSCluster cluster;
  private HRegion r;
  private Path oldLogPath = new Path("/old-logs");
  
  public TestCompactionCompletionDuringRecovery() {
    super();
    conf.setBoolean("dfs.support.append", true);
    this.cluster = null;
  }

  @Override
  public void setUp() throws Exception {
    this.cluster = new MiniDFSCluster(conf, 2, true, (String[])null);
    this.conf.set(HConstants.HBASE_DIR,
      this.cluster.getFileSystem().getHomeDirectory().toString());
    super.setUp();
    HTableDescriptor htd = createTableDescriptor(getName());
    this.r = createNewHRegion(htd, null, null);
  }
  
  @Override
  public void tearDown() throws Exception {
    HLog hlog = r.getLog();
    this.r.close();
    hlog.closeAndDelete();
    if (this.cluster != null) {
      shutdownDfs(cluster);
    }
    super.tearDown();
  }

  private void makeStoreFiles(HRegion r, int numFiles) throws IOException {
    Put p = new Put(Bytes.toBytes("row"));
    p.add(fam1, Bytes.toBytes("qualifier"), Bytes.toBytes("value"));
    for (int i = 0; i < numFiles; i++) {
      r.put(p);
      r.flushcache();
    }
  }
  
  public void testFaultBeforeRename() throws IOException {
    makeStoreFiles(r, 5);

    FaultInjector.inject(FaultInjector.Point.COMPACTION_BEFORE_RENAME,
        FaultInjector.THROW_IOEXCEPTION);
    try {
      Path storeDir = r.getStore(fam1).getHomeDir();
      assertEquals(5, fs.listStatus(storeDir).length);
      try {
        r.compactStores(true);
        fail("Fault did not cause IOE to bubble up!");
      } catch (IOException ioe) {
        LOG.info("Expected", ioe);
      }
      FaultInjector.clear();
      
      HLog hlog = r.getLog();
      this.r.close();
      hlog.close();
  
      assertEquals(5, fs.listStatus(storeDir).length);
      HLog.splitLog(this.testDir, hlog.getDir(), oldLogPath, this.fs, this.conf);
      assertEquals(1, fs.listStatus(storeDir).length);    
    } finally {
      FaultInjector.clear();
    }
  }
  

  public void testFaultBeforeDelete() throws IOException {
    makeStoreFiles(r, 5);

    FaultInjector.inject(FaultInjector.Point.COMPACTION_DELETE_OLD,
        FaultInjector.throwOnNthInvocation(3));
    try {
      Path storeDir = r.getStore(fam1).getHomeDir();
      assertEquals(5, fs.listStatus(storeDir).length);
      try {
        r.compactStores(true);
        fail("Fault did not cause IOE to bubble up!");
      } catch (IOException ioe) {
        LOG.info("Expected", ioe);
      }
      FaultInjector.clear();
      
      HLog hlog = r.getLog();
      this.r.close();
      hlog.close();
  
      // We now have fewer files, since we got past deleting 2 of them
      assertEquals(3, fs.listStatus(storeDir).length);
      HLog.splitLog(this.testDir, hlog.getDir(), oldLogPath, this.fs, this.conf);
      // Splitting the log finishes us back to the 1 that we expect
      assertEquals(1, fs.listStatus(storeDir).length);    
    } finally {
      FaultInjector.clear();
    }
  }

}
