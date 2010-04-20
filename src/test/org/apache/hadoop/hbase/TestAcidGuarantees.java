/**
 * Copyright 2007 The Apache Software Foundation
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

import java.util.Random;
import java.util.Set;
import java.util.HashSet;
import java.io.IOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestAcidGuarantees {
  protected static final Log LOG = LogFactory.getLog(TestAcidGuarantees.class);
  public static final byte [] TABLE_NAME = Bytes.toBytes("TestTable");
  public static final byte [] FAMILY_A = Bytes.toBytes("A");
  public static final byte [] FAMILY_B = Bytes.toBytes("B");
  public static final byte [] FAMILY_C = Bytes.toBytes("C");
  public static final byte [] QUALIFIER_NAME = Bytes.toBytes("data");

  public static int NUM_COLS_TO_CHECK = 50;

  protected static final HTableDescriptor TABLE_DESCRIPTOR;
  static {
    TABLE_DESCRIPTOR = new HTableDescriptor(TABLE_NAME);
    TABLE_DESCRIPTOR.addFamily(new HColumnDescriptor(FAMILY_A));
    TABLE_DESCRIPTOR.addFamily(new HColumnDescriptor(FAMILY_B));
    TABLE_DESCRIPTOR.addFamily(new HColumnDescriptor(FAMILY_C));
  }
 

  private void createTableIfMissing(HBaseConfiguration conf)
    throws IOException {
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      admin.createTable(TABLE_DESCRIPTOR);
      LOG.info("Created table!");
    } catch (TableExistsException tee) {
    }
  }

  public static class TestContext {
    private final HBaseConfiguration conf;
    private Throwable err = null;
    private boolean stopped = false;
    private int threadDoneCount = 0;
    private Set<TestThread> testThreads = new HashSet<TestThread>();

    public TestContext(HBaseConfiguration conf) {
      this.conf = conf;
    }
    public HBaseConfiguration getConf() {
      return conf;
    }
    public synchronized boolean shouldRun()  {
      return !stopped && err == null;
    }

    public void addThread(TestThread t) {
      testThreads.add(t);
    }

    public void startThreads() {
      for (TestThread t : testThreads) {
        t.start();
      }
    }

    public void waitFor(long millis) throws Exception {
      long endTime = System.currentTimeMillis() + millis;
      while (!stopped) {
        long left = endTime - System.currentTimeMillis();
        if (left <= 0) break;
        synchronized (this) {
          wait(left);
          checkException();
        }
      }
    }
    private synchronized void checkException() throws Exception {
      if (err != null) {
        throw new RuntimeException("Deferred", err);
      }
    }

    public synchronized void threadFailed(Throwable t) {
      if (err == null) err = t;
      testThreads.remove(Thread.currentThread());
      notify();
    }

    public synchronized void threadDone() {
      threadDoneCount++;
    }

    public void stop() throws InterruptedException {
      synchronized (this) {
        stopped = true;
      }
      for (TestThread t : testThreads) {
        t.join();
      }
    }
  }

  public static abstract class TestThread extends Thread {
    final TestContext ctx;
    protected boolean stopped;

    public TestThread(TestContext ctx) {
      this.ctx = ctx;
    }

    public void run() {
      try {
        while (ctx.shouldRun() && !stopped) {
          doAnAction();
        }
      } catch (Throwable t) {
        ctx.threadFailed(t);
      }
      ctx.threadDone();
    }

    protected void stopTestThread() {
      this.stopped = true;
    }

    public abstract void doAnAction() throws Exception;
  }

  public static class AtomicityWriter extends TestThread {
    Random rand = new Random();
    byte data[] = new byte[10];
    byte targetRow[];
    HTable table;

    public AtomicityWriter(TestContext ctx, byte targetRow[]) throws IOException {
      super(ctx);
      this.targetRow = targetRow;
      table = new HTable(ctx.getConf(), TABLE_NAME);
    }
    public void doAnAction() throws Exception {
      Put p = new Put(targetRow); 
      rand.nextBytes(data);

      for (int i = 0; i < NUM_COLS_TO_CHECK; i++) {
        byte qualifier[] = Bytes.toBytes("col" + i);
        p.add(FAMILY_A, qualifier, data);
      }
      table.put(p);
    }
  }
  
  public static class AtomicityReader extends TestThread {
    byte targetRow[];
    HTable table;
    int numVerified = 0;

    public AtomicityReader(TestContext ctx, byte targetRow[]) throws IOException {
      super(ctx);
      this.targetRow = targetRow;
      table = new HTable(ctx.getConf(), TABLE_NAME);
    }

    public void doAnAction() throws Exception {
      Get g = new Get(targetRow);
      Result res = table.get(g);
      byte[] gotValue = null;

      for (int i = 0; i < NUM_COLS_TO_CHECK; i++) {
        byte qualifier[] = Bytes.toBytes("col" + i);
        byte thisValue[] = res.getValue(FAMILY_A, qualifier);
        if (gotValue != null && !Bytes.equals(gotValue, thisValue)) {
          gotFailure(gotValue, res);
        }
        numVerified++;
        gotValue = thisValue;
      }
    }

    private void gotFailure(byte[] expected, Result res) {
      StringBuilder msg = new StringBuilder();
      msg.append("Failed after ").append(numVerified).append("!");
      msg.append("Expected=").append(Bytes.toStringBinary(expected));
      msg.append("Got:\n");
      for (KeyValue kv : res.list()) {
        msg.append("key=").append(kv.getKeyString());
        msg.append(" val=").append(Bytes.toStringBinary(kv.getValue()));
        msg.append(" ts=").append(kv.getTimestamp());
        msg.append("\n");
      }
      throw new RuntimeException(msg.toString());
    }
  }



  public void runTestAtomicity(HBaseConfiguration conf) throws Exception {
    createTableIfMissing(conf);
    TestContext ctx = new TestContext(conf);
    byte row[] = Bytes.toBytes("test_row");
    AtomicityWriter writer = new AtomicityWriter(ctx, row);

    ctx.addThread(writer);
    ctx.addThread(new AtomicityReader(ctx, row));
    ctx.startThreads();
    ctx.waitFor(60000);
    ctx.stop();
  }

  public static void main(String args[]) throws Exception {
    HBaseConfiguration c = new HBaseConfiguration();
    new TestAcidGuarantees().runTestAtomicity(c);
  }
}
