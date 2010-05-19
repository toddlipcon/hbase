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

import java.io.PrintWriter;

import org.cliffc.high_scale_lib.Counter;

public class BinnedHistogram<K> {
  private Counter[] bins;
  private Binner<K> binner;

  public BinnedHistogram(Binner<K> binner) {
	this.binner = binner;
    this.bins = new Counter[binner.getNumBins()];
    for (int i = 0; i < bins.length; i++) {
  	  bins[i] = new Counter();
    }
  }
  
  public void incr(K value) {
    bins[binner.getBin(value)].increment();
  }
  
  public void incr(K value, long delta) {
    bins[binner.getBin(value)].add(delta);
  }
  
  public void clear() {
    for (Counter bin : bins) {
  	  bin.set(0);
    }
  }
  
  public long[] snapshot() {
    long ret[] = new long[bins.length];
    int i = 0;
    for (Counter bin : bins) {
  	  ret[i++] = bin.get();
    }
    return ret;
  }
  
  public long[] estimatedSnapshot() {
    long ret[] = new long[bins.length];
    int i = 0;
    for (Counter bin : bins) {
  	  ret[i++] = bin.estimate_get();
    }
    return ret;
  }

  public void dump(PrintWriter out) {
	long snap[] = snapshot();
	assert snap.length == binner.getNumBins();
	for (int bin = 0; bin < snap.length; bin++) {
	  if (snap[bin] == 0) continue;
	  out.print(binner.getBinLabel(bin));
	  out.print(": ");
	  
	  out.print(": ");
	  out.println(snap[bin]);	  
	}
  }
}