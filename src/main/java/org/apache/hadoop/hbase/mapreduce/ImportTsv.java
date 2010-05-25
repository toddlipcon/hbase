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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Tool to import data from a TSV file.
 * @see ImportTsv#usage(String)
 */
public class ImportTsv {
  final static String NAME = "importtsv";

  /**
   * Write table content out to files in hdfs.
   */
  static class TsvImporter
  extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>
  {
    /**
     * Column families and qualifiers mapped to the TSV columns
     */
    private byte[][] families;
    private byte[][] qualifiers;
    
    /** Timestamp for all inserted rows */
    private long ts;

    @Override
    protected void setup(Context context) {
      String columnStrings[] = context.getConfiguration().getStrings("columns");
      families = new byte[columnStrings.length][];
      qualifiers = new byte[columnStrings.length][];

      for (int i = 0; i < columnStrings.length; i++) {
        String str = columnStrings[i];
        String[] parts = str.split(":", 2);
        if (parts.length == 1) {
          families[i] = columnStrings[i].getBytes();
          qualifiers[i] = HConstants.EMPTY_BYTE_ARRAY;
        } else {
          families[i] = parts[0].getBytes();
          qualifiers[i] = parts[1].getBytes();
        }
      }
      
      ts = System.currentTimeMillis();
    }

    /**
     * Convert a line of TSV text into an HBase table row.
     */
    @Override
    public void map(LongWritable offset, Text value,
      Context context)
    throws IOException {
      try {
        byte[] lineBytes = value.getBytes();
        LinkedList<Integer> tabOffsets = new LinkedList<Integer>();
        for (int i = 0; i < value.getLength(); i++) {
          if (lineBytes[i] == '\t') {
            tabOffsets.add(i);
          }
        }
        tabOffsets.add(lineBytes.length);

        int rowkeySep = tabOffsets.remove();
        ImmutableBytesWritable rowKey =
          new ImmutableBytesWritable(lineBytes, 0, rowkeySep);
        
        int lastOff = rowkeySep;
        int colIdx = 0;
        Put put = new Put(rowKey.copyBytes());
        for (int tabOff : tabOffsets) {
          KeyValue kv = new KeyValue(
              lineBytes, 0, rowkeySep, // row key
              families[colIdx], 0, families[colIdx].length,
              qualifiers[colIdx], 0, qualifiers[colIdx].length,
              ts,
              KeyValue.Type.Put,
              lineBytes, lastOff + 1, tabOff - lastOff - 1);
          put.add(kv);
          lastOff = tabOff;
          colIdx++;
        }
        context.write(rowKey, put);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Sets up the actual job.
   *
   * @param conf  The current configuration.
   * @param args  The command line parameters.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  public static Job createSubmittableJob(Configuration conf, String[] args)
  throws IOException {
    String tableName = args[0];
    Path inputDir = new Path(args[1]);
    Job job = new Job(conf, NAME + "_" + tableName);
    job.setJarByClass(TsvImporter.class);
    FileInputFormat.setInputPaths(job, inputDir);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(TsvImporter.class);

    if (conf.getBoolean("use.hfile", false)) {
      HTable table = new HTable(conf, tableName);
      job.setReducerClass(PutSortReducer.class);
      // TODO need to be able to specify a path here, this is pretty
      // ghetto!
      Path outputDir = new Path("hfof." + tableName);
      FileOutputFormat.setOutputPath(job, outputDir);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(Put.class);
      HFileOutputFormat.configureIncrementalLoad(job, table);
    } else {
      // No reducers.  Just write straight to table.  Call initTableReducerJob
      // to set up the TableOutputFormat.
      TableMapReduceUtil.initTableReducerJob(tableName, null, job);
      job.setNumReduceTasks(0);
    }
    
    TableMapReduceUtil.addDependencyJars(job);
    return job;
  }

  /*
   * @param errorMsg Error message.  Can be null.
   */
  private static void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println(
        "Usage: ImportTsv -Dcolumns=a,b,c <tablename> <inputdir>");
  }

  /**
   * Main entry point.
   *
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      usage("Wrong number of arguments: " + otherArgs.length);
      System.exit(-1);
    }
    String columns[] = conf.getStrings("columns");
    if (columns == null) {
      usage("No columns specified. Please specify with -Dcolumns=...");
    }
    Job job = createSubmittableJob(conf, otherArgs);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
