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


import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



@SuppressWarnings("unchecked")
public class PerfCounters {
  static final Log LOG = LogFactory.getLog(PerfCounters.class);
  
  final ConcurrentMap<String, BinnedHistogram> histograms =
	new ConcurrentHashMap<String, BinnedHistogram>();
  
  static PerfCounters instance = new PerfCounters();
    
  public static PerfCounters get() {
	return instance;
  }
  
  public <K> BinnedHistogram<K> addHistogram(String key, BinnedHistogram<K> histogram) {
	BinnedHistogram<?> putResult = histograms.putIfAbsent(key, histogram);
	if (putResult != null) {
	  LOG.warn("Already initialized histogram " + key, new Exception());
	
	}
	return histogram;
  }

  public BinnedHistogram getHistogram(String key) {
	return histograms.get(key);
  }
  
  public void dump(PrintWriter out) throws IOException { 
	for (Map.Entry<String, BinnedHistogram> entry :
	  		histograms.entrySet()) {
	  out.print(entry.getKey());
	  out.print(":\n");
	  entry.getValue().dump(out);
	}
  }
}
