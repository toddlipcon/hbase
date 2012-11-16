package org.apache.hadoop.hbase.io.hfile.flash;

import org.apache.hadoop.hbase.io.hfile.CacheStats;
import java.util.concurrent.atomic.AtomicLong;

public class FlashCacheStats extends CacheStats {
	  /** The number of getBlock requests that hit the inflight RAM cache */
	  private final AtomicLong inflightHitCount = new AtomicLong(0);
	
	  /** The number of getBlock requests that hit the flash cache */
	  private final AtomicLong flashHitCount = new AtomicLong(0);

	  /** The number of times the clock ran */
	  private final AtomicLong clockRanCount = new AtomicLong(0);

	  /** The number of times the clock did heavy lifting */
	  private final AtomicLong clockInsufficientSpaceCount = new AtomicLong(0);

	  /** The number of blocks dropped by the clock */
	  private final AtomicLong clockDroppedCount = new AtomicLong(0);

}
