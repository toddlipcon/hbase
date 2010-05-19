/**
 * 
 */
package org.apache.hadoop.hbase.perf;

public interface Binner<K> {

  public int getNumBins();
  public int getBin(K value);
  public String getBinLabel(int bin);

  
  public static class LinearLongBinner implements Binner<Long> {
    private final long minVal;
    private final long binWidth;
    
    /**
     * Number of bins *not* including the <min and >max bins
     */
    private final int numBins;
    
    public LinearLongBinner(long minVal, long binWidth, int numBins) {
      this.minVal = minVal;
      this.binWidth = binWidth;
      this.numBins = numBins;
    }
  
    @Override
    public String getBinLabel(int bin) {
      if (bin == 0) {
    	return "<" + minVal;
      } else if (bin == numBins + 1) {
    	return ">" + (minVal + numBins * binWidth);
      } else {
    	long binMin = (bin - 1)*binWidth + minVal;
    	return binMin + "-" + (binMin + binWidth);
      }
    }
  
    @Override
    public int getBin(Long value) {
      if (value < minVal) return 0;
      return Math.min((int)((value - minVal) / binWidth), numBins) + 1;
    }
  
    @Override
    public int getNumBins() {
      return numBins + 2;
    }
  }
  
  public static class LogLongBinner implements Binner<Long> {
    private final long minVal;
    private final double exponentBase, logExponentBase;
    
    /**
     * Number of bins *not* including the <min and >max bins
     */
    private final int numBins;
    
    public LogLongBinner(long minVal, double exponentBase, int numBins) {
      this.minVal = minVal;
      this.exponentBase = exponentBase;
      this.logExponentBase = Math.log(exponentBase);
      this.numBins = numBins;
    }
  
    @Override
    public String getBinLabel(int bin) {
      if (bin == 0) {
    	return "<" + minVal;
      } else if (bin == numBins + 1) {
    	long binMin = (long) (minVal * Math.pow(exponentBase, bin));
    	return ">" + binMin;
      } else {
    	long binMin = (long) (minVal * Math.pow(exponentBase, bin));
    	long binNext = (long) (minVal * Math.pow(exponentBase, bin + 1));
    	return binMin + "-" + binNext;
      }
    }
  
    @Override
    public int getBin(Long value) {
      if (value < minVal) return 0;
      double rawVal = Math.log((double)value / (double)minVal) / logExponentBase;
      return Math.min((int)rawVal, numBins) + 1;
    }
  
    @Override
    public int getNumBins() {
      return numBins + 2;
    }
  }

}