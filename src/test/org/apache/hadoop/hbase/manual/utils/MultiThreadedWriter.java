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
package org.apache.hadoop.hbase.manual.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.manual.HBaseTest;

public class MultiThreadedWriter extends MultiThreadedAction
{
  private static final Log LOG = LogFactory.getLog(MultiThreadedWriter.class);
  static long minColumnsPerKey_ = 1;
  static long maxColumnsPerKey_ = 10;
  static int minDataSize_ = 256;
  static int maxDataSize_ = 1024;
  Set<HBaseWriter> writers_ = new HashSet<HBaseWriter>();
  static boolean bulkLoad_ = false;
  /* This is the current key to be inserted by any thread. Each thread does an 
     atomic get and increment operation and inserts the current value. */
  public static AtomicLong currentKey_ = null;
  /* The sorted set of keys inserted by the writers */
  public static SortedSet<Long> insertedKeySet_ = Collections.synchronizedSortedSet(new TreeSet<Long>());
  /* The sorted set of keys NOT inserted by the writers */
  public static SortedSet<Long> failedKeySet_ = Collections.synchronizedSortedSet(new TreeSet<Long>());

  public MultiThreadedWriter(byte[] tableName, byte[] columnFamily) {
    tableName_ = tableName;
    columnFamily_ = columnFamily;
  }

  public void setBulkLoad(boolean bulkLoad) {
    bulkLoad_ = bulkLoad;
  }

  public void setColumnsPerKey(long minColumnsPerKey, long maxColumnsPerKey) {
    minColumnsPerKey_ = minColumnsPerKey;
    maxColumnsPerKey_ = maxColumnsPerKey;
  }

  public void setDataSize(int minDataSize, int maxDataSize) {
    minDataSize_ = minDataSize;
    maxDataSize_ = maxDataSize;
  }

  public void start(long startKey, long endKey, int numThreads) {
    if(verbose_) {
      LOG.debug("Inserting keys [" + startKey + ", " + endKey + ")");
    }
    startKey_ = startKey;
    endKey_ = endKey;
    numThreads_ = numThreads;
    currentKey_ = new AtomicLong(startKey_);

    for(int i = 0; i < numThreads_; ++i) {
      HBaseWriter writer = new HBaseWriter(i);
      writers_.add(writer);
    }
    numThreadsWorking_ = new AtomicInteger(writers_.size());
    for(HBaseWriter writer : writers_) {
      writer.start();
    }

    startReporter();
  }

  public void waitForFinish() {
    while(numThreadsWorking_.get() != 0) {
      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public static class HBaseWriter extends Thread {
    int id_;
    Random random_ = new Random();
    List<HTable> tables_ = new ArrayList<HTable>();
    static DataGenerator dataGenerator_ = new DataGenerator(minDataSize_, maxDataSize_);

    public HBaseWriter(int id) {
      id_ = id;
      for(HBaseConfiguration conf : HBaseTest.configList_) {
//        try {
          HTable table = HBaseUtils.getHTable(conf, tableName_);
//          LOG.info("setAutoFlush is default");
//          table.setAutoFlush(false);
//          table.setWriteBufferSize(1024*1024*12);
//          table.setScannerCaching(30);
          tables_.add(table);
//        } catch(IOException e) {
//          e.printStackTrace();
//        }
      }
    }

    public void run() {
      if(MultiThreadedWriter.bulkLoad_) {
        long rowKey = currentKey_.getAndIncrement();
        do {
          long numColumns = minColumnsPerKey_ + Math.abs(random_.nextLong())%(maxColumnsPerKey_-minColumnsPerKey_);
          bulkInsertKey(rowKey, 0, numColumns);
          rowKey = currentKey_.getAndIncrement();
        } while(rowKey < endKey_);
      }
      else {
        long rowKey = currentKey_.getAndIncrement();
        do {
          long numColumns = minColumnsPerKey_ + Math.abs(random_.nextLong())%(maxColumnsPerKey_-minColumnsPerKey_);
          for(long col = 0; col < numColumns; ++col) {
            insert(rowKey, col);
          }
          rowKey = currentKey_.getAndIncrement();
        } while(rowKey < endKey_);
      }
      numThreadsWorking_.decrementAndGet();
    }
    
    public static byte[] longToByteArrayKey(long rowKey) {
      return DataGenerator.paddedKey(rowKey).getBytes();
    }

    public void insert(long rowKey, long col) {
      Put put = new Put(longToByteArrayKey(rowKey));
      put.add(columnFamily_, ("" + col).getBytes(), dataGenerator_.getDataInSize(rowKey));
      try {
        long start = System.currentTimeMillis();
        putIntoTables(put);
        insertedKeySet_.add(rowKey);
        numKeys_.addAndGet(1);
        numCols_.addAndGet(1);
        cumulativeOpTime_.addAndGet(System.currentTimeMillis() - start);
      }
      catch (IOException e) {
        failedKeySet_.add(rowKey);
        e.printStackTrace();
      }
    }

    public void bulkInsertKey(long rowKey, long startCol, long endCol) {
      if (verbose_) {
         LOG.debug("Preparing put for key = " + rowKey + ", cols = [" + startCol + ", " + endCol + ")");
      }

      if(startCol >= endCol) {
        return;
      }
      Put put = new Put(DataGenerator.paddedKey(rowKey).getBytes());
      byte[] columnQualifier;
      byte[] value;
      for(long i = startCol; i < endCol; ++i) {
        value = dataGenerator_.getDataInSize(rowKey);
        columnQualifier = ("" + i).getBytes();
        put.add(columnFamily_, columnQualifier, value);
      }
      try {
        long start = System.currentTimeMillis();
        putIntoTables(put);
        insertedKeySet_.add(rowKey);
        numKeys_.addAndGet(1);
        numCols_.addAndGet(endCol - startCol);
        cumulativeOpTime_.addAndGet(System.currentTimeMillis() - start);
      }
      catch (IOException e) {
        failedKeySet_.add(rowKey);
        e.printStackTrace();
      }
    }

    // error handling correct only for ONE table
    public void putIntoTables(Put put) throws IOException {
      for(HTable table : tables_) {
        table.put(put);
      }
    }
  }
}
