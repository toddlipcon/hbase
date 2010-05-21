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
package org.apache.hadoop.hbase.perf;

import java.util.Map;
import java.util.TreeMap;

import org.cliffc.high_scale_lib.Counter;

import com.google.common.base.Function;
import com.google.common.collect.MapMaker;

public class CounterMap<K> {
  private Map<K, Counter> counters;
  
  public CounterMap() {
    counters = new MapMaker().makeComputingMap(
  	new Function<K, Counter>() {
		  public Counter apply(K counterKey) {
			return new Counter();		
		  };
		});	
  }
  
  public void incr(String counter) {
    counters.get(counter).increment();
  }

  public void incr(String counter, int delta) {
    counters.get(counter).add(delta);
  }
  public void decr(String counter) {
    counters.get(counter).decrement();
  }

  public Map<K, Long> snapshot() {
    Map<K, Long> ret = new TreeMap<K, Long>();
    for (Map.Entry<K, Counter> entry : counters.entrySet()) {
       ret.put(entry.getKey(), entry.getValue().get());
    }
    return ret;
  }

  public Map<K, Long> estimatedSnapshot() {
    Map<K, Long> ret = new TreeMap<K, Long>();
    for (Map.Entry<K, Counter> entry : counters.entrySet()) {
  	  ret.put(entry.getKey(), entry.getValue().estimate_get());
    }
    return ret;	
  }
}