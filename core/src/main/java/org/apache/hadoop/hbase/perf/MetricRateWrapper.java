package org.apache.hadoop.hbase.perf;

import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

public class MetricRateWrapper extends MetricsTimeVaryingRate {

  private BinnedHistogram<Long> histogram;
  
  public MetricRateWrapper(String nam, MetricsRegistry registry) {
	super(nam, registry);
	histogram = new BinnedHistogram<Long>(
		new Binner.LogLongBinner(1, 1.1, 150));
	PerfCounters.get().addHistogram(nam, histogram);
  }
  
  @Override
  public void inc(int numOps, long time) {
	histogram.incr(time, numOps);
	super.inc(numOps, time);
  }
  
  @Override
  public void inc(long time) {
	histogram.incr(time);
	super.inc(time);
  }

}
