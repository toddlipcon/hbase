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
import org.apache.hadoop.hbase.MultithreadedTestUtil.TestThread;
import org.apache.hadoop.hbase.MultithreadedTestUtil.TestContext;


public class TestAcidGuarantees {
  protected static final Log LOG = LogFactory.getLog(TestAcidGuarantees.class);
  public static final byte [] TABLE_NAME = Bytes.toBytes("TestTable");
  public static final byte [] FAMILY_A = Bytes.toBytes("A");
  public static final byte [] FAMILY_B = Bytes.toBytes("B");
  public static final byte [] FAMILY_C = Bytes.toBytes("C");
  public static final byte [] QUALIFIER_NAME = Bytes.toBytes("data");

  public static final byte[][] FAMILIES = new byte[][] {
    FAMILY_A, FAMILY_B, FAMILY_C };

  private HBaseTestingUtility util;

  public static int NUM_COLS_TO_CHECK = 50;

  private void createTableIfMissing()
    throws IOException {
    try {
      util.createTable(TABLE_NAME, FAMILIES);
    } catch (TableExistsException tee) {
    }
  }

  public TestAcidGuarantees(HBaseConfiguration conf) {
    util = new HBaseTestingUtility(conf);
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
        msg.append(kv.toString());
        msg.append(" val= ");
        msg.append(Bytes.toStringBinary(kv.getValue()));
        msg.append("\n");
      }
      throw new RuntimeException(msg.toString());
    }
  }


  public void runTestAtomicity() throws Exception {
    createTableIfMissing();
    TestContext ctx = new TestContext(util.getConfiguration());
    byte row[] = Bytes.toBytes("test_row");

    ctx.addThread(new AtomicityWriter(ctx, row));
    ctx.addThread(new AtomicityReader(ctx, row));
    ctx.startThreads();
    ctx.waitFor(300000);
    ctx.stop();
  }

  public static void main(String args[]) throws Exception {
    HBaseConfiguration c = new HBaseConfiguration();
    new TestAcidGuarantees(c).runTestAtomicity();
  }
}
