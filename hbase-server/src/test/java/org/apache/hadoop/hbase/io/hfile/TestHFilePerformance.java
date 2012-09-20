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
package org.apache.hadoop.hbase.io.hfile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.experimental.categories.Category;

/**
 *  Set of long-running tests to measure performance of HFile.
 * <p>
 * Copied from
 * <a href="https://issues.apache.org/jira/browse/HADOOP-3315">hadoop-3315 tfile</a>.
 * Remove after tfile is committed and use the tfile version of this class
 * instead.</p>
 */
@Category(MediumTests.class)
public class TestHFilePerformance extends TestCase {
  private static final int BLOCK_SIZE = 64 * 1024;
  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static String ROOT_DIR =
    TEST_UTIL.getDataTestDir("TestHFilePerformance").toString();
  private FileSystem fs;
  private Configuration conf;
  private long startTimeEpoch;
  private long finishTimeEpoch;
  private DateFormat formatter;

  @Override
  public void setUp() throws IOException {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf).getRaw();
    formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  }

  public void startTime() {
    startTimeEpoch = System.currentTimeMillis();
    System.out.println(formatTime() + " Started timing.");
  }

  public void stopTime() {
    finishTimeEpoch = System.currentTimeMillis();
    System.out.println(formatTime() + " Stopped timing.");
  }

  public long getIntervalMillis() {
    return finishTimeEpoch - startTimeEpoch;
  }

  public void printlnWithTimestamp(String message) {
    System.out.println(formatTime() + "  " +  message);
  }

  /*
   * Format millis into minutes and seconds.
   */
  public String formatTime(long milis){
    return formatter.format(milis);
  }

  public String formatTime(){
    return formatTime(System.currentTimeMillis());
  }

  private FSDataOutputStream createFSOutput(Path name) throws IOException {
    if (fs.exists(name))
      fs.delete(name, true);
    FSDataOutputStream fout = fs.create(name);
    return fout;
  }

  //TODO have multiple ways of generating key/value e.g. dictionary words
  //TODO to have a sample compressable data, for now, made 1 out of 3 values random
  //     keys are all random.

  private static class KeyValueGenerator {
    byte[] randomValueData = new byte[BLOCK_SIZE * 2];
    
    int valueSequence = 0 ;
    long keySequence = 0;
    
    static final float COMPRESSION_RATIO = 0.5f;
    static final int KEY_SIZE = 100;
    
    KeyValueGenerator() {
      Random valueRandomizer = new Random(0L); //TODO with seed zero
      
      byte[] compressibleChunk = new byte[(int)(KEY_SIZE * COMPRESSION_RATIO)];

      int outPos = 0;
      while (outPos < randomValueData.length) {
        valueRandomizer.nextBytes(compressibleChunk);
        
        int chunkRem = Math.min(randomValueData.length - outPos, 100);
        while (chunkRem > 0) {
          int copyLen = Math.min(chunkRem, compressibleChunk.length);
          System.arraycopy(compressibleChunk, 0, randomValueData, outPos, copyLen);
          outPos += copyLen;
          chunkRem -= copyLen;
        }
      }
    }

    // Key is always random now.
    void getKey(byte[] key) {
      Bytes.putLong(key, key.length - 8, keySequence++);
    }

    void getValue(byte[] value) {
      if (valueSequence + value.length > randomValueData.length) {
        valueSequence = 0;
      }
      System.arraycopy(randomValueData, valueSequence, value, 0, value.length);
      valueSequence += value.length;
    }
  }

  /**
   *
   * @param fileType "HFile" or "SequenceFile"
   * @param keyLength
   * @param valueLength
   * @param codecName "none", "lzo", "gz", "snappy"
   * @param rows number of rows to be written.
   * @param writeMethod used for HFile only.
   * @param minBlockSize used for HFile only.
   * @throws IOException
   */
   //TODO writeMethod: implement multiple ways of writing e.g. A) known length (no chunk) B) using a buffer and streaming (for many chunks).
  public void timeWrite(String fileType, int keyLength, int valueLength,
    String codecName, long rows, String writeMethod, int minBlockSize,
    DataBlockEncoding encoding)
  throws IOException {
    System.out.println("File Type: " + fileType);
    System.out.println("Writing " + fileType + " with codecName: " + codecName);
    long totalBytesWritten = 0;


    //Using separate randomizer for key/value with seeds matching Sequence File.
    byte[] key = new byte[keyLength];
    byte[] key2 = new byte[keyLength];
    byte[] value = new byte[valueLength];
    KeyValueGenerator generator = new KeyValueGenerator();

    startTime();

    Path path = new Path(ROOT_DIR, fileType + ".Performance");
    System.out.println(ROOT_DIR + path.getName());
    FSDataOutputStream fout =  createFSOutput(path);

    if ("HFile".equals(fileType)){
        System.out.println("HFile write method: ");
        HFile.Writer writer = HFile.getWriterFactoryNoCache(conf)
            .withOutputStream(fout)
            .withBlockSize(minBlockSize)
            .withCompression(codecName)
            .withDataBlockEncoder(new HFileDataBlockEncoderImpl(encoding))
            .create();

        // Writing value in one shot.
        for (long l=0; l<rows; l++ ) {
          generator.getKey(key);
          generator.getValue(value);
          writer.append(key, value);
          totalBytesWritten += key.length;
          totalBytesWritten += value.length;
          
          byte[] tmp = key2;
          key2 = key;
          key = tmp;
         }
        writer.close();
    } else if ("SequenceFile".equals(fileType)){
        CompressionCodec codec = null;
        if ("gz".equals(codecName))
          codec = new GzipCodec();
        else if (!"none".equals(codecName))
          throw new IOException("Codec not supported.");

        SequenceFile.Writer writer;

        //TODO
        //JobConf conf = new JobConf();

        if (!"none".equals(codecName))
          writer = SequenceFile.createWriter(conf, fout, BytesWritable.class,
            BytesWritable.class, SequenceFile.CompressionType.BLOCK, codec);
        else
          writer = SequenceFile.createWriter(conf, fout, BytesWritable.class,
            BytesWritable.class, SequenceFile.CompressionType.NONE, null);

        BytesWritable keyBsw;
        BytesWritable valBsw;
        for (long l=0; l<rows; l++ ) {

           generator.getKey(key);
           keyBsw = new BytesWritable(key);
           totalBytesWritten += keyBsw.getSize();

           generator.getValue(value);
           valBsw = new BytesWritable(value);
           writer.append(keyBsw, valBsw);
           totalBytesWritten += valBsw.getSize();
        }

        writer.close();
    } else
       throw new IOException("File Type is not supported");

    fout.close();
    stopTime();

    printlnWithTimestamp("Data written: ");
    printlnWithTimestamp("  rate  = " +
      totalBytesWritten / getIntervalMillis() * 1000 / 1024 / 1024 + "MB/s");
    printlnWithTimestamp("        = " +
      rows * 1000 / getIntervalMillis() + "rows/sec");
    printlnWithTimestamp("  total = " + totalBytesWritten + "B");

    printlnWithTimestamp("File written: ");
    printlnWithTimestamp("  rate  = " +
      fs.getFileStatus(path).getLen() / getIntervalMillis() * 1000 / 1024 / 1024 + "MB/s");
    printlnWithTimestamp("  total = " + fs.getFileStatus(path).getLen() + "B");
  }

  public void timeReading(String fileType, int keyLength, int valueLength,
      long rows, int method) throws IOException {
    System.out.println("Reading file of type: " + fileType);
    Path path = new Path(ROOT_DIR, fileType + ".Performance");
    System.out.println("Input file size: " + fs.getFileStatus(path).getLen());
    long totalBytesRead = 0;


    ByteBuffer val;

    ByteBuffer key;

    startTime();
    FSDataInputStream fin = fs.open(path);

    if ("HFile".equals(fileType)){
        HFile.Reader reader = HFile.createReaderFromStream(path, fs.open(path),
          fs.getFileStatus(path).getLen(), new CacheConfig(conf));
        reader.loadFileInfo();
        switch (method) {

          case 0:
          case 1:
          default:
            {
              HFileScanner scanner = reader.getScanner(false, false);
              scanner.seekTo();
              for (long l=0; l<rows; l++ ) {
                KeyValue kv = scanner.getKeyValue();
                totalBytesRead += kv.getKeyLength() + kv.getValueLength();
                scanner.next();
              }
            }
            break;
        }
      reader.close();
    } else if("SequenceFile".equals(fileType)){

        SequenceFile.Reader reader;
        reader = new SequenceFile.Reader(fs, path, new Configuration());

        if (reader.getCompressionCodec() != null) {
            printlnWithTimestamp("Compression codec class: " + reader.getCompressionCodec().getClass());
        } else
            printlnWithTimestamp("Compression codec class: " + "none");

        BytesWritable keyBsw = new BytesWritable();
        BytesWritable valBsw = new BytesWritable();

        for (long l=0; l<rows; l++ ) {
          reader.next(keyBsw, valBsw);
          totalBytesRead += keyBsw.getSize() + valBsw.getSize();
        }
        reader.close();

        //TODO make a tests for other types of SequenceFile reading scenarios

    } else {
        throw new IOException("File Type not supported.");
    }


    //printlnWithTimestamp("Closing reader");
    fin.close();
    stopTime();
    //printlnWithTimestamp("Finished close");

    printlnWithTimestamp("Finished in " + getIntervalMillis() + "ms");
    printlnWithTimestamp("Data read: ");
    printlnWithTimestamp("  rate  = " +
      totalBytesRead / getIntervalMillis() * 1000 / 1024 / 1024 + "MB/s");
    printlnWithTimestamp("        = " +
        rows * 1000 / getIntervalMillis() + "rows/sec");
    printlnWithTimestamp("  total = " + totalBytesRead + "B");

    printlnWithTimestamp("File read: ");
    printlnWithTimestamp("  rate  = " +
      fs.getFileStatus(path).getLen() / getIntervalMillis() * 1000 / 1024 / 1024 + "MB/s");
    printlnWithTimestamp("  total = " + fs.getFileStatus(path).getLen() + "B");

    //TODO uncomment this for final committing so test files is removed.
    //fs.delete(path, true);
  }

  public void testRunComparisons() throws IOException {

    int keyLength = 16;
    int valueLength = 100;
    int minBlockSize = BLOCK_SIZE;
    int rows = 10000000;

    System.out.println("\n\n\n");
    System.out.println("****************************** HFile *****************************");

    timeWrite("HFile", keyLength, valueLength, "none", rows, null, minBlockSize,
        DataBlockEncoding.PREFIX);
    System.out.println("\n+++++++\n");
    for (int i = 0; i < 40; i++) {
      timeReading("HFile", keyLength, valueLength, rows, 0 );
    }

    System.out.println("");
    System.out.println("----------------------");
    System.out.println("");
/* DISABLED LZO
    timeWrite("HFile", keyLength, valueLength, "lzo", rows, null, minBlockSize);
    System.out.println("\n+++++++\n");
    timeReading("HFile", keyLength, valueLength, rows, 0 );
    System.out.println("\n+++++++\n");
    timeReading("HFile", keyLength, valueLength, rows, 1 );
    System.out.println("\n+++++++\n");
    timeReading("HFile", keyLength, valueLength, rows, 2 );

    System.out.println("");
    System.out.println("----------------------");
    System.out.println("");
*/
    //timeWrite("HFile", keyLength, valueLength, "gz", rows, null, minBlockSize);
    System.out.println("\n+++++++\n");
    //timeReading("HFile", keyLength, valueLength, rows, 0 );

    System.out.println("\n\n\n\nNotes: ");
    System.out.println(" * Timing includes open/closing of files.");
    System.out.println(" * Timing includes reading both Key and Value");
    System.out.println(" * Data is generated as random bytes. Other methods e.g. using " +
            "dictionary with care for distributation of words is under development.");
    System.out.println(" * Timing of write currently, includes random value/key generations. " +
            "Which is the same for Sequence File and HFile. Another possibility is to generate " +
            "test data beforehand");
    System.out.println(" * We need to mitigate cache effect on benchmark. We can apply several " +
            "ideas, for next step we do a large dummy read between benchmark read to dismantle " +
            "caching of data. Renaming of file may be helpful. We can have a loop that reads with" +
            " the same method several times and flood cache every time and average it to get a" +
            " better number.");
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
  
  public static void main(String []args) throws IOException {
    TestHFilePerformance test = new TestHFilePerformance();
    test.setUp();
    test.testRunComparisons();
  }
}

