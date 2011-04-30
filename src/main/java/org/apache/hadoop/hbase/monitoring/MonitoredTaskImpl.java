package org.apache.hadoop.hbase.monitoring;

public class MonitoredTaskImpl implements MonitoredTask {
  private long startTime;
  private long completionTimestamp = -1;
  
  private float progress = Float.NaN;
  private String status;
  private String description;
  
  private State state = State.RUNNING;
  
  public MonitoredTaskImpl() {
    startTime = System.currentTimeMillis();
  }

  @Override
  public long getStartTime() {
    return startTime;
  }
  
  @Override
  public String getDescription() {
    return description;
  }
  
  @Override
  public float getProgress() {
    return progress;
  }
  
  @Override
  public String getStatus() {
    return status;
  }
  
  @Override
  public State getState() {
    return state;
  }
  
  @Override
  public long getCompletionTimestamp() {
    return completionTimestamp;
  }
  
  @Override
  public void setProgress(float progress) {
    this.progress = progress;
  }

  @Override
  public void markComplete(String status) {
    state = State.COMPLETE;
    setStatus(status);
    setProgress(1.0f);
    completionTimestamp = System.currentTimeMillis();
  }
  
  @Override
  public void setStatus(String status) {
    this.status = status;
  }

  @Override
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public void cleanup() {
    if (state == State.RUNNING) {
      state = State.ABORTED;
      completionTimestamp = System.currentTimeMillis();
    }
  }
}
