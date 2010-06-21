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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKMap;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class TestZooKeeper {
  private final Log LOG = LogFactory.getLog(this.getClass());

  private final static HBaseTestingUtility
      TEST_UTIL = new HBaseTestingUtility();

  private static Configuration conf;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Test we can first start the ZK cluster by itself
    TEST_UTIL.startMiniZKCluster();
    //TEST_UTIL.getConfiguration().setBoolean("dfs.support.append", true);
    //TEST_UTIL.startMiniCluster(2);
    conf = TEST_UTIL.getConfiguration();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    //TEST_UTIL.ensureSomeRegionServersAvailable(2);
  }

  /**
   * See HBASE-1232 and http://wiki.apache.org/hadoop/ZooKeeper/FAQ#4.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testClientSessionExpired()
      throws IOException, InterruptedException {
    new HTable(conf, HConstants.META_TABLE_NAME);

    ZooKeeperWrapper zkw =
        ZooKeeperWrapper.createInstance(conf, TestZooKeeper.class.getName());
    zkw.registerListener(EmptyWatcher.instance);
    String quorumServers = zkw.getQuorumServers();
    int sessionTimeout = 5 * 1000; // 5 seconds
    HConnection connection = HConnectionManager.getConnection(conf);
    ZooKeeperWrapper connectionZK = connection.getZooKeeperWrapper();
    long sessionID = connectionZK.getSessionID();
    byte[] password = connectionZK.getSessionPassword();

    ZooKeeper zk = new ZooKeeper(quorumServers, sessionTimeout,
        EmptyWatcher.instance, sessionID, password);
    zk.close();

    Thread.sleep(sessionTimeout * 3L);

    System.err.println("ZooKeeper should have timed out");
    connection.relocateRegion(HConstants.ROOT_TABLE_NAME, HConstants.EMPTY_BYTE_ARRAY);
  }
  @Test
  public void testRegionServerSessionExpired() throws Exception{
    LOG.info("Starting testRegionServerSessionExpired");
    new HTable(conf, HConstants.META_TABLE_NAME);
    TEST_UTIL.expireRegionServerSession(0);
    testSanity();
  }
  @Test
  public void testMasterSessionExpired() throws Exception {
    LOG.info("Starting testMasterSessionExpired");
    new HTable(conf, HConstants.META_TABLE_NAME);
    TEST_UTIL.expireMasterSession();
    testSanity();
  }

  /**
   * Make sure we can use the cluster
   * @throws Exception
   */
  public void testSanity() throws Exception{

    HBaseAdmin admin = new HBaseAdmin(conf);
    String tableName = "test"+System.currentTimeMillis();
    HTableDescriptor desc =
        new HTableDescriptor(tableName);
    HColumnDescriptor family = new HColumnDescriptor("fam");
    desc.addFamily(family);
    admin.createTable(desc);

    HTable table = new HTable(tableName);
    Put put = new Put(Bytes.toBytes("testrow"));
    put.add(Bytes.toBytes("fam"),
        Bytes.toBytes("col"), Bytes.toBytes("testdata"));
    table.put(put);

  }

  @Test
  public void testMultipleZK() {
    try {
      HTable localMeta = new HTable(conf, HConstants.META_TABLE_NAME);
      Configuration otherConf = HBaseConfiguration.create(conf);
      otherConf.set(HConstants.ZOOKEEPER_QUORUM, "127.0.0.1");
      HTable ipMeta = new HTable(otherConf, HConstants.META_TABLE_NAME);

      // dummy, just to open the connection
      localMeta.exists(new Get(HConstants.LAST_ROW));
      ipMeta.exists(new Get(HConstants.LAST_ROW));

      // make sure they aren't the same
      assertFalse(HConnectionManager.getClientZooKeeperWatcher(conf)
          .getZooKeeperWrapper() == HConnectionManager.getClientZooKeeperWatcher(
          otherConf).getZooKeeperWrapper());
      assertFalse(HConnectionManager.getConnection(conf)
          .getZooKeeperWrapper().getQuorumServers().equals(HConnectionManager
          .getConnection(otherConf).getZooKeeperWrapper().getQuorumServers()));
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * Create a bunch of znodes in a hierarchy, try deleting one that has childs
   * (it will fail), then delete it recursively, then delete the last znode
   * @throws Exception
   */
  @Test
  public void testZNodeDeletes() throws Exception {
    ZooKeeperWrapper zkw =
        ZooKeeperWrapper.createInstance(conf, TestZooKeeper.class.getName());
    zkw.registerListener(EmptyWatcher.instance);
    zkw.ensureExists("/l1/l2/l3/l4");
    try {
      zkw.deleteZNode("/l1/l2");
      fail("We should not be able to delete if znode has childs");
    } catch (KeeperException ex) {
      assertNotNull(zkw.getData("/l1/l2/l3", "l4"));
    }
    zkw.deleteZNode("/l1/l2", true);
    assertNull(zkw.getData("/l1/l2/l3", "l4"));
    zkw.deleteZNode("/l1");
    assertNull(zkw.getData("/l1", "l2"));
  }
  
  @Test
  public void testZKMap() throws Exception {
    ZooKeeperWrapper zkw1 =
      ZooKeeperWrapper.createInstance(conf, TestZooKeeper.class.getName());
    ZooKeeper zk1 = zkw1.getZooKeeper();
    ZooKeeperWrapper zkw2 =
      ZooKeeperWrapper.createInstance(conf, TestZooKeeper.class.getName() + "_2");
    ZooKeeper zk2 = zkw2.getZooKeeper();

    byte[] data1 = Bytes.toBytes("hello world");
    byte[] data2 = Bytes.toBytes("goodbye world");

    zkw1.ensureExists("/mymap");
    ZKMap zkm1 = new ZKMap(zk1, "/mymap", "zkm 1");
    assertTrue(zkm1.putIfAbsent("hello", data1));

    ZKMap zkm2 = new ZKMap(zk2, "/mymap", "zkm 2");
    assertFalse(zkm2.putIfAbsent("hello", data1));
    
    zkm2.waitForSynchronization(1000);
    assertArrayEquals(data1, zkm2.get("hello"));
    
    Pair<byte[], Stat> pairFrom1 = zkm1.getWithStat("hello");
    Pair<byte[], Stat> pairFrom2 = zkm2.getWithStat("hello");
    assertArrayEquals(data1, pairFrom2.getFirst());
    assertTrue(zkm2.putIfUnchanged("hello", data2, pairFrom2.getSecond()));
    assertFalse(zkm1.putIfUnchanged("hello", data2, pairFrom1.getSecond()));
    zkm1.waitForSynchronization(1000);
    assertArrayEquals(data2, zkm1.get("hello"));
    assertArrayEquals(data2, zkm2.get("hello"));
  }
}