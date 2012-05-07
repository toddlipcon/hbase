/**
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

import java.util.ArrayList;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.HConcurrentSkipListMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

/**
 * Micro-benchmark for performance of KV comparison
 */
@Category(SmallTests.class)
public class TestKVComparisonPerformance {
  
  Random rng = new Random();
    
    @Test
  public void testPerformance() {
    ArrayList<KeyValue> kvs = Lists.newArrayList();
    byte[] family = new byte[1];
    byte[] qualifier = new byte[1];
    byte[] value = new byte[20];
    
    // Create 2M key values in an array
    // Each pair of adjacent values differs only in their timestamp
    // but are otherwise random.
    for (int i = 0; i < 1000000; i++) {
      byte[] row = new byte[rng.nextInt(10)];

      rng.nextBytes(row);
      rng.nextBytes(family);
      rng.nextBytes(qualifier);
      rng.nextBytes(value);
      
      long ts = 10;
      KeyValue kv = new KeyValue(row, family, qualifier,
          ts, KeyValue.Type.Put, value);
      kvs.add(kv);

      ts += 15;
      kv = new KeyValue(row, family, qualifier,
          ts, KeyValue.Type.Put, value);
      kvs.add(kv);
    }

    int TRIALS = 100;
    long sum = 0;
    
    for (int i = 0; i < TRIALS; i++) {
      NavigableMap<KeyValue, KeyValue> map;
      if (i % 2 == 0) {
        map = new ConcurrentSkipListMap<KeyValue, KeyValue>(KeyValue.COMPARATOR);
      } else {
        map = new TreeMap<KeyValue, KeyValue>(KeyValue.COMPARATOR);
      }
      KeyValueSkipListSet kvset = new KeyValueSkipListSet(map);
      System.gc();
      System.gc();

      long st = System.nanoTime();
      kvset.addAll(kvs);
      long total = 0;
      for (KeyValue kv : kvset) {
        total += kv.hashCode();
      }
      long et = System.nanoTime();
      long us = TimeUnit.MICROSECONDS.convert(et-st, TimeUnit.NANOSECONDS);
      System.out.println(map.getClass().getSimpleName()+":" +
          "Run " + i + " took " + us + "us total=" + total);
      
      if (i > 10) {
        sum += us;
      }
      kvset = null;
    }

    System.out.println("Avg: " + (sum / (TRIALS - 10)) + "us");
  }
}
