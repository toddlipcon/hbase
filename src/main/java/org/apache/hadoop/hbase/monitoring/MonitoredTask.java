package org.apache.hadoop.hbase.monitoring;

public interface MonitoredTask {

  enum State {
    RUNNING,
    COMPLETE,
    ABORTED;
  }
 

  public abstract long getStartTime();

  public abstract String getDescription();

  public abstract float getProgress();

  public abstract String getStatus();

  public abstract State getState();

  public abstract long getCompletionTimestamp();

  public abstract void setProgress(float progress);

  public abstract void markComplete(String status);

  public abstract void setStatus(String status);

  public abstract void setDescription(String description);

  /**
   * Explicitly mark this status as able to be cleaned up,
   * even though it might not be complete.
   */
  public abstract void cleanup();

}