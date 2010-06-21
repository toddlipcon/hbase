package org.apache.hadoop.hbase.zookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.google.common.collect.Lists;

public abstract class ZKAction<T> {
  protected Watcher watcher = null;

  private List<Pair<Class<? extends Throwable>, T>>
    exceptionReturnMap = Lists.newLinkedList();
  
  public static ZKAction<Boolean> exists(String znode) {
    return new ZKExists(znode);
  }
  
  public static ZKAction<byte[]> getData(String znode) {
    return new ZKGetData(znode); 
  }  
  
  public ZKAction<T> withWatcher(Watcher watcher) {
    this.watcher = watcher;
    return this;
  }
  
  public ZKAction<T> onExceptionReturn(
      Class<? extends Throwable> exClass, T toReturn) {
    this.exceptionReturnMap.add(
        new Pair<Class<? extends Throwable>, T>(exClass, toReturn));
    return this;
  }
  
  public T doOrThrowRTE(ZooKeeper zk) {
    try {
      return doAndCheckExceptions(zk);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  public T doOrThrowIOE(ZooKeeper zk) throws IOException {
    try {
      return doAndCheckExceptions(zk);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
  
  public T doAndCheckExceptions(ZooKeeper zk)
  throws KeeperException, InterruptedException {
    try {
      return doIt(zk);
    } catch (KeeperException e) {
      try {
        return replaceOrRethrowException(e);
      } catch (UnreplacedException e1) {
        throw e;
      }
    } catch (InterruptedException e) {
      try {
        return replaceOrRethrowException(e);
      } catch (UnreplacedException e1) {
        throw e;
      }
    }
  }
  
  private T replaceOrRethrowException(Throwable exc)
    throws UnreplacedException {
    for (Pair<Class<? extends Throwable>, T> pair : exceptionReturnMap) {
      if (pair.getFirst().isAssignableFrom(exc.getClass())) {
        return pair.getSecond();
      }
    }
    throw new UnreplacedException(exc);
  }
  
  private static class UnreplacedException extends Exception {
    private static final long serialVersionUID = 1L;

    UnreplacedException(Throwable e) {
      super(e);
    }
  }
    
  public static class ZKExists extends ZKAction<Boolean> {
    private final String node;
    public ZKExists(String node) {
      this.node = node;
    }
    
    public Boolean doIt(ZooKeeper zk)
    throws KeeperException, InterruptedException {
      return zk.exists(node, watcher) != null;
    }
  }

  public static class ZKGetData extends ZKAction<byte[]> {
    private final String node;
    public ZKGetData(String node) {
      this.node = node;
    }
    
    public byte[] doIt(ZooKeeper zk)
    throws KeeperException, InterruptedException {
      return zk.getData(node, watcher, null);
    }
  }
  
  protected abstract T doIt(ZooKeeper zk)
    throws KeeperException, InterruptedException;


  
  
  
}
