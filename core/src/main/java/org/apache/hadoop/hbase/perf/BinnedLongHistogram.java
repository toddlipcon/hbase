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

import java.io.PrintStream;
import java.io.PrintWriter;

import org.cliffc.high_scale_lib.Counter;

public class BinnedLongHistogram {
  private long minVal;
  private int numBins;
  private long binWidth;
  private Counter[] bins;

  public BinnedLongHistogram(long minVal, long binWidth, int numBins) {
    this.minVal = minVal;
    this.numBins = numBins;
    this.binWidth = binWidth;
    this.bins = new Counter[numBins + 2]; // 2 for <min, >max
    for (int i = 0; i < bins.length; i++) {
  	  bins[i] = new Counter();
    }
  }
  
  public void incr(long value) {
    bins[binForValue(value)].increment();
  }
  
  public void incr(long value, long delta) {
    bins[binForValue(value)].add(delta);
  }
  
  private int binForValue(long value) {
    if (value < minVal) return 0;
    return Math.min((int)((value - minVal) / binWidth), numBins) + 1;
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
	assert snap.length == numBins + 2;
	for (int bin = 0; bin < snap.length; bin++) {
	  if (bin == 0) {
		out.print("<" + minVal);
	  } else if (bin == numBins + 1) {
		out.print(">" + (minVal + numBins * binWidth));
	  } else {
		long binMin = (bin - 1)*binWidth + minVal;
		out.print(binMin + "-" + (binMin + binWidth));
	  }
	  out.print(": ");
	  out.println(snap[bin]);	  
	}
  } 
}