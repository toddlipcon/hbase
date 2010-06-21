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
package org.apache.hadoop.hbase.zookeeper;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestZKMap {
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
    conf = TEST_UTIL.getConfiguration();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
  
  @Test
  public void testZKMap() throws Exception {
    ZooKeeperWrapper zkw1 =
      ZooKeeperWrapper.createInstance(conf, TestZKMap.class.getName());
    ZooKeeper zk1 = zkw1.getZooKeeper();
    ZooKeeperWrapper zkw2 =
      ZooKeeperWrapper.createInstance(conf, TestZKMap.class.getName() + "_2");
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

    assertTrue(zkm2.remove("hello"));
    assertFalse(zkm2.remove("hello"));
    
    assertTrue(zkm2.waitForSynchronization(1000));
    assertFalse(zkm2.containsKey("hello"));
    
    assertTrue(zkm1.waitForSynchronization(1000));
    assertFalse(zkm1.containsKey("hello"));
  }
}
