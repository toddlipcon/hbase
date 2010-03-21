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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.reflect.Constructor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.hbase.util.MurmurHash;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.LineReader;


/**
 * Script used evaluating HBase performance and scalability.  Runs a HBase
 * client that steps through one of a set of hardcoded tests or 'experiments'
 * (e.g. a random reads test, a random writes test, etc.). Pass on the
 * command-line which test to run and how many clients are participating in
 * this experiment. Run <code>java VerifiableEditor --help</code> to
 * obtain usage.
 * 
 * <p>This class sets up and runs the evaluation programs described in
 * Section 7, <i>Performance Evaluation</i>, of the <a
 * href="http://labs.google.com/papers/bigtable.html">Bigtable</a>
 * paper, pages 8-10.
 * 
 * <p>If number of clients > 1, we start up a MapReduce job. Each map task
 * runs an individual client. Each client does about 1GB of data.
 */
public class VerifiableEditor implements HConstants {
  protected static final Log LOG = LogFactory.getLog(VerifiableEditor.class.getName());
  
  private static final int ROW_LENGTH = 1000;
  private static final int ONE_GB = 1024 * 1024 * 1000;
  private static final int ROWS_PER_GB = ONE_GB / ROW_LENGTH;
  
  public static final byte [] TABLE_NAME = Bytes.toBytes("TestTable");
  public static final byte [] FAMILY_NAME = Bytes.toBytes("info");
  public static final byte [] QUALIFIER_NAME = Bytes.toBytes("data");

  protected static final HTableDescriptor TABLE_DESCRIPTOR;
  static {
    TABLE_DESCRIPTOR = new HTableDescriptor(TABLE_NAME);
    TABLE_DESCRIPTOR.addFamily(new HColumnDescriptor(FAMILY_NAME));
  }

  volatile HBaseConfiguration conf;

  /**
   * Constructor
   * @param c Configuration object
   */
  public VerifiableEditor(final HBaseConfiguration c) {
    this.conf = c;
  }

  private byte [] getDataToWrite(String clientId, long curWrite) {
    return Bytes.toBytes(String.valueOf(curWrite) + "<" + clientId + ">");
  }

  private class Writer implements Callable<Integer> {
    private long randomSeed;
    private String clientId;

    public Writer(String args[]) {
      this.randomSeed = System.currentTimeMillis();
      this.clientId = generateClientId();
    }

    private String generateClientId() {
      assert randomSeed != 0;
      return String.valueOf(randomSeed); // TODO better
    }
  
    private RandomAccessFile openLocalRecorder() throws IOException {
      RandomAccessFile localRecorder = new RandomAccessFile(
        "/dev/shm/hbase-verifiableeditor-" + clientId,
        "rws");
      localRecorder.seek(0);
      localRecorder.writeLong(randomSeed);
      localRecorder.writeLong(-1);
      return localRecorder;
    }
  
    private void recordIteration(RandomAccessFile raf,
                                 long iteration) throws IOException {
      raf.seek(8);
      raf.writeLong(iteration);
    }
 

    public Integer call() throws IOException  {
      createTableIfMissing();
      doWrites();
      return 0;
    }

    private void doWrites() throws IOException {
      RandomAccessFile recorder = openLocalRecorder();
      HTable table = new HTable(conf, TABLE_NAME);
      Random r = new Random(randomSeed);
      boolean stop = false;
      long iteration = 0;
      while (!stop) {
        int curWrite = r.nextInt();
        byte[] curData = getDataToWrite(clientId, curWrite);
        Put p = new Put(curData /* row */);
        p.add(FAMILY_NAME, QUALIFIER_NAME, curData);
        table.put(p);

        recordIteration(recorder, iteration);
        iteration++;

        if (iteration % 1000 == 0) {
          LOG.info("Client " + clientId + " written " + iteration + " iterations");
        }
      }
    }
  }

  private class Verifier implements Callable<Integer> {
    private final long randomSeed;
    private final long verifyUpTo;

    public Verifier(List<String> args) throws IOException {
      if (args.size() != 1) {
        printUsage();
        throw new RuntimeException("bad usage");
      }

      DataInputStream in = new DataInputStream(new FileInputStream(args.get(0)));
      try {
        randomSeed = in.readLong();
        verifyUpTo = in.readLong();
      } finally {
        in.close();
      }
    }

    public Integer call() throws IOException {
      HTable table = new HTable(conf, TABLE_NAME);
      Random r = new Random(randomSeed);
      String clientId = String.valueOf(randomSeed);
      for (long i = 0; i < verifyUpTo; i++) {
        int curWrite = r.nextInt();
        byte[] curData = getDataToWrite(clientId, curWrite);
        Get g = new Get(curData);
        Result res = table.get(g);
        byte[] gotValue = res.getValue(FAMILY_NAME, QUALIFIER_NAME);
        if (! Bytes.equals(curData, gotValue)) {
          throw new RuntimeException("VERIFICATION FAILED. iteration=" + i + "seed=" + randomSeed);
        }
      }

      LOG.info("Successfully verified " + verifyUpTo + " writes from " + randomSeed);

      return 0;
    }
  }
 

  protected void printUsage() {
    printUsage(null);
  }
  
  protected void printUsage(final String message) {
    if (message != null && message.length() > 0) {
      System.err.println(message);
    }
    System.err.println("Usage: java " + this.getClass().getName() + " \\");
    System.err.println(" [writer | verify <writerlog>]");
  }

  private void createTableIfMissing() throws IOException {
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      admin.createTable(TABLE_DESCRIPTOR);
      LOG.info("Created table!");
    } catch (TableExistsException tee) {
    }
  }



  public int doCommandLine(String args[]) {
    if (args.length < 1) {
      printUsage();
      return 1;
    }

    List<String> toolArgs = Arrays.<String>asList(args).subList(1, args.length);

    try {
      Callable<Integer> tool = null;

      // Pick tool
      if (args[0].equals("writer")) {
        tool = new Writer(args);
      } else if (args[0].equals("verify")) {
        tool = new Verifier(toolArgs);
      } else {
        printUsage("unknown tool: " + args[0]);
        return 1;
      }

      return tool.call();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * @param args
   */
  public static void main(final String[] args) {
    HBaseConfiguration c = new HBaseConfiguration();
    System.exit(new VerifiableEditor(c).doCommandLine(args));
  }
}
