package org.apache.hadoop.hbase.io.hfile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.NavigableSet;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStore.ScanInfo;
import org.apache.hadoop.hbase.regionserver.KeyValueHeap;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScanQueryMatcher;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.Reader;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.mockito.Mockito;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;

public class TestStorePerformance {
  
  private static final Log LOG = LogFactory.getLog(TestStorePerformance.class);
  
  public static class Args {
    @Parameter(names = {"-hfile-block-size"}, description = "HFile block size")
    Integer HFILE_BLOCK_SIZE = 64 * 1024;
    
    @Parameter(names = {"-key-size"}, description = "Size of keys to insert")
    Integer KEY_SIZE = 16;
    
    @Parameter(names = {"-value-size"}, description = "Size of values to insert")
    Integer VALUE_SIZE = 100;
    
    @Parameter(names = {"-num-cells"}, description = "Number of cells to insert")
    Integer NUM_ROWS = 100000;
    
    @Parameter(names = {"-write-file"}, description = "Path to write")
    String WRITE_FILE = null;
    
    @Parameter(names = {"-read-raw"}, description = "Read the HFiles directly, not through StoreScanner and heap")
    boolean READ_RAW = false;
    
    @Parameter(names = {"-read-file"}, description = "Paths to read")
    List<String> READ_FILES = Lists.newArrayList();
    
    @Parameter(names = {"-encoding"}, description = "Encoding to use")
    String BLOCK_ENCODING = DataBlockEncoding.NONE.name();
    
    Float COMPRESSION_RATIO = 0.5f;

    @Parameter(names = {"-use-raw-fs"}, description = "Use raw (non-checksummed) fs when writing to local FS")
    public boolean USE_RAW_FS = true;
    
    @Parameter(names = {"-disable-checksums"}, description = "Disable use of HBase checksums")
    public boolean DISABLE_CHECKSUMS = false;
    
    @Parameter(names = {"-repeat-reads"}, description = "Number of times to loop the read benchmark")
    int READ_REPETITIONS = 5;
  }

  private final Args args;
  private KeyValueGenerator kvgen;
  private Configuration conf;
  private CacheConfig cacheConf;
  private FileSystem fs;
  
  public TestStorePerformance() {
    this(new Args());
  }
  
  
  private TestStorePerformance(Args args) {
    this.args = args;
    this.kvgen = new KeyValueGenerator(args.HFILE_BLOCK_SIZE,
        args.VALUE_SIZE,
        args.COMPRESSION_RATIO,
        args.NUM_ROWS);
    this.conf = new Configuration();
    // disable block cache
    conf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY,
        0);
    
    this.cacheConf = new CacheConfig(conf);
    
    
    SchemaMetrics.setUseTableNameInTest(false);
  }
  
  private void doWrite() throws Exception {
    Path p = new Path(args.WRITE_FILE);
    this.fs = getFs(p);
    
    byte[] key = new byte[args.KEY_SIZE];
    byte[] val = new byte[args.VALUE_SIZE];
    
    // Layout: row cf qual
    int cfLen = 1;
    int qualLen = 1;
    int rowLen = key.length - cfLen - qualLen;
    int cfOffset = rowLen;
    int qualOffset = rowLen + cfLen;
    
    long ts = 0;
    
    StoreFile.Writer w = new StoreFile.WriterBuilder(
        conf, cacheConf, fs, args.HFILE_BLOCK_SIZE)
      .withFilePath(p)
      .withDataBlockEncoder(getEncoder())
      .build();
    try {
      for (int i = 0; i < args.NUM_ROWS; ++i) {
        kvgen.getKey(key);
        kvgen.getValue(val);

        KeyValue kv = new KeyValue(
            key, 0, rowLen,
            key, cfOffset, cfLen,
            key, qualOffset, qualLen,
            ts++,
            Type.Put,
            val, 0, val.length);
        w.append(kv);
      }
    } finally {
      IOUtils.closeStream(w);
    }
  }
  
  private HFileDataBlockEncoder getEncoder() {
    for (DataBlockEncoding enc : DataBlockEncoding.values()) {
      if (enc.name().equals(args.BLOCK_ENCODING)) {
        return new HFileDataBlockEncoderImpl(enc);        
      }
    }
    throw new IllegalArgumentException(
        "Invalid encoding: " + args.BLOCK_ENCODING + ". Valid values: " +
         Joiner.on(", ").join(DataBlockEncoding.values()));
  }


  private FileSystem getFs(Path p) throws IOException {
    FileSystem fs = p.getFileSystem(conf);
    if (fs instanceof LocalFileSystem && args.USE_RAW_FS) {
      fs = ((LocalFileSystem)fs).getRaw();
    }
    return new HFileSystem(fs, !args.DISABLE_CHECKSUMS);
  }


  private void run() throws Exception {
    if (args.WRITE_FILE != null) {
      doWrite();
    }
    
    for (int i = 1; i <= args.READ_REPETITIONS; i++) {
      LOG.info("Beginning read #" + i);
      if (args.READ_RAW) {
        for (String s : args.READ_FILES) {
          doRawRead(s);
        }
      } else {
        doRead(args.READ_FILES);
      }
    }
  }

  private void doRawRead(String path) throws IOException {
    Path p = new Path(path);
    FileSystem fs = getFs(p);
    long totalBytesRead = 0;
    long count = 0;

    Stopwatch sw = new Stopwatch();
    HFile.Reader reader = HFile.createReader(fs, p, cacheConf);
    try {
      HFileScanner scanner = reader.getScanner(false, false);
      sw.start();
      
      boolean hasNext = scanner.seekTo();
      while (hasNext) {
        ByteBuffer k = scanner.getKey();
        ByteBuffer v = scanner.getValue();
        
        totalBytesRead += k.limit() + v.limit();
        count++;
        hasNext = scanner.next();
      }
      sw.stop();
    } finally {
      reader.close();
    }
    
    long elapsedMillis = sw.elapsedMillis() + 1;
    long rowsPerSec = (count * 1000 / elapsedMillis);

    LOG.info("Read " + count + " raw rows in " + elapsedMillis + " ms");
    LOG.info(rowsPerSec + " rows per second");

  }

  private void doRead(List<String> paths) throws IOException {
    List<KeyValueScanner> scanners = Lists.newArrayList();

    Scan scan = new Scan();
    
    
    byte[] family = new byte[0];
    int minVersions = 1;
    int maxVersions = 1;
    long ttl = Long.MAX_VALUE;
    boolean keepDeletedCells = false;
    long timeToPurgeDeletes = 0;
    KVComparator comparator = KeyValue.COMPARATOR;
    ScanInfo scanInfo = new ScanInfo(family, minVersions, maxVersions, ttl, keepDeletedCells, timeToPurgeDeletes, comparator);
    NavigableSet<byte[]> columns = null;
    for (String pStr : paths) {
      Path p = new Path(pStr);
      FileSystem fs = getFs(p);
      Reader reader = new StoreFile.Reader(fs, p, cacheConf,
          DataBlockEncoding.NONE);
      StoreFileScanner scanner = reader.getStoreFileScanner(false, false);
      scanners.add(scanner);
    }
    
    StoreScanner ss = new StoreScanner(scan, scanInfo, ScanType.USER_SCAN, columns, scanners);
    
    long count = 0;
    long keyBytes = 0, valBytes = 0;
    Stopwatch sw = new Stopwatch().start();
    try {
      List<KeyValue> vals = Lists.newArrayList();
      while (ss.next(vals, 10)) {
        for (KeyValue kv : vals) {
          keyBytes += kv.getKeyLength();
          valBytes += kv.getValueLength();
          count++;
        }
        vals.clear();
      }

      sw.stop();
    } finally {
      ss.close();
    }
    long elapsedMillis = sw.elapsedMillis() + 1;
    long rowsPerSec = (count * 1000 / elapsedMillis);
    LOG.info("Read " + count + " rows in " + elapsedMillis + " ms");
    LOG.info(rowsPerSec + " rows per second");
  }

  private static class KeyValueGenerator {
    final byte[] randomValueData;
    
    final long keyIncrement;
    int valueSequence = 0 ;
    long keySequence = 0;
    
    KeyValueGenerator(int blockSize, int valueSize, float compressibility,
        int totalRows) {
      
      keyIncrement = Long.MAX_VALUE / totalRows;
      
      this.randomValueData = new byte[blockSize* 2];
      Random valueRandomizer = new Random(0L);
      
      byte[] compressibleChunk = new byte[(int)(valueSize * compressibility)];

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
      Bytes.putLong(key, key.length - 8, keySequence);
      keySequence += keyIncrement;
    }

    void getValue(byte[] value) {
      if (valueSequence + value.length > randomValueData.length) {
        valueSequence = 0;
      }
      System.arraycopy(randomValueData, valueSequence, value, 0, value.length);
      valueSequence += value.length;
    }
  }


  public static void main(String []argv) throws Exception {
    Args args = new Args();
    JCommander jc = new JCommander(args);
    try {
      jc.parse(argv);
    } catch (Exception e) {
      System.err.println(e.getMessage());
      jc.usage();
      System.exit(-1);
    }

    new TestStorePerformance(args).run();
  }

}
