package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class TestWideScanner extends HBaseTestCase {
  private final Log LOG = LogFactory.getLog(this.getClass());

  static final int BATCH = 1000;

  private MiniDFSCluster cluster = null;
  private HRegion r;

  static final HTableDescriptor TESTTABLEDESC =
    new HTableDescriptor("testwidescan");
  static {
    TESTTABLEDESC.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY,
      10,  // Ten is arbitrary number.  Keep versions to help debuggging.
      Compression.Algorithm.NONE.getName(), false, true, 8 * 1024,
      HConstants.FOREVER, false, HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
  }
  /** HRegionInfo for root region */
  public static final HRegionInfo REGION_INFO =
    new HRegionInfo(TESTTABLEDESC, HConstants.EMPTY_BYTE_ARRAY,
    HConstants.EMPTY_BYTE_ARRAY);

  @Override
  public void setUp() throws Exception {
    cluster = new MiniDFSCluster(conf, 2, true, (String[])null);
    // Set the hbase.rootdir to be the home directory in mini dfs.
    this.conf.set(HConstants.HBASE_DIR,
      this.cluster.getFileSystem().getHomeDirectory().toString());
    super.setUp();    
  }

  private int addWideContent(HRegion region, byte[] family) 
      throws IOException {
    int count = 0;
    // add a few rows of 2500 columns (we'll use batch of 1000) to make things
    // interesting
    for (char c = 'a'; c <= 'c'; c++) {
      byte[] row = Bytes.toBytes("ab" + c);
      int i;
      for (i = 0; i < 2500; i++) {
        byte[] b = Bytes.toBytes(String.format("%10d", i));
        Put put = new Put(row);
        put.add(family, b, b);
        region.put(put);
        count++;
      }
    }
    // add one row of 100,000 columns
    {
      byte[] row = Bytes.toBytes("abf");
      int i;
      for (i = 0; i < 100000; i++) {
        byte[] b = Bytes.toBytes(String.format("%10d", i));
        Put put = new Put(row);
        put.add(family, b, b);
        region.put(put);
        count++;
      }
    }
    return count;
  }

  public void testWideScanBatching() throws IOException {
    try {
      this.r = createNewHRegion(REGION_INFO.getTableDesc(), null, null);
      int inserted = addWideContent(this.r, HConstants.CATALOG_FAMILY);
      List<KeyValue> results = new ArrayList<KeyValue>();
      Scan scan = new Scan();
      scan.addFamily(HConstants.CATALOG_FAMILY);
      scan.setBatch(BATCH);
      InternalScanner s = r.getScanner(scan);
      int total = 0;
      int i = 0;
      boolean more;
      do {
        more = s.next(results);
        i++;
        LOG.info("iteration #" + i + ", results.size=" + results.size());

        // assert that the result set is no larger than BATCH
        assertTrue(results.size() <= BATCH);

        total += results.size();

        if (results.size() > 0) {
          // assert that all results are from the same row
          byte[] row = results.get(0).getRow();
          for (KeyValue kv: results) {
            assertTrue(Bytes.equals(row, kv.getRow()));
          }
        }

        results.clear();
      } while (more);

      // assert that the scanner returned all values
      LOG.info("inserted " + inserted + ", scanned " + total);
      assertTrue(total == inserted);

      s.close();
    } finally {
      this.r.close();
      this.r.getLog().closeAndDelete();
      shutdownDfs(this.cluster);
    }
  }
}
