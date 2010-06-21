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
package org.apache.hadoop.hbase.zookeeper;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.google.common.collect.Sets;


public class ZKMap implements Watcher {
  private static final Log LOG = LogFactory.getLog(ZKMap.class);
  private final ZooKeeper zk;
  private final String path;
  private final String name;
  
  private Map<String, Pair<byte[], Stat>> map =
    Collections.synchronizedMap(new HashMap<String, Pair<byte[], Stat>>());

  
  private volatile boolean waitingForChildren;
  private Set<String> waitingForData = Collections.synchronizedSet(
      new HashSet<String>());
  private volatile String errorString = null;
  private Object newDataArrived = new Object();
  
  public ZKMap(ZooKeeper zk, String path, String name)
    throws KeeperException, InterruptedException {    
    this.zk = zk;
    this.path = path;
    this.name = name;
    updateAndWatchChildren();
  }
  
  public byte[] get(String key) {
    checkError();
    Pair<byte[], Stat> pair = map.get(key);
    if (pair == null) return null;
    return pair.getFirst();
  }
  
  public Pair<byte[], Stat> getWithStat(String key) {
    checkError();
    return map.get(key);
  }
  
  public boolean putIfAbsent(String key, byte[] data) {
    checkError();
    try {
      zk.create(path + "/" + key, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch (KeeperException.NodeExistsException e) {
      return false;
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return true;
  }
  
  public boolean putIfUnchanged(String key, byte[] newData, Stat oldStat) {
    checkError();
    try {
      zk.setData(keyToZpath(key), newData, oldStat.getVersion());
    } catch (KeeperException.BadVersionException e) {
      return false;
    } catch (KeeperException.NoNodeException nne) {
      return false;
    } catch (KeeperException ke) {
      throw new RuntimeException(ke);
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
    return true;
  }
  
  public boolean remove(String key) {
    checkError();
    try {
      zk.delete(keyToZpath(key), -1);
      LOG.info("Removed key " + key);
      return true;
    } catch (KeeperException.NoNodeException nne) {
      return false;
    } catch (KeeperException ke) {
      throw new RuntimeException(ke);
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }
  
  public boolean containsKey(String key) {
    checkError();
    return map.containsKey(key);
  }

  @Override
  public void process(WatchedEvent event) {
    LOG.info("[" + name + "] ZKMap watcher processing " + event);
    if (path.equals(event.getPath())) {
      // Children changed on the top node
      switch (event.getType()) {
      case NodeChildrenChanged:
        break;
      default:
        markDataStructureBroken("Bad event type: " + event.getType() + " on " + path);
        break;
      }
      updateAndWatchChildren();
    } else if (event.getPath().startsWith(path)) {
      // A child event
      switch (event.getType()) {
      case NodeDataChanged:
        updateAndWatchKey(zpathToKey(event.getPath()));
        break;
      case NodeDeleted:
        removeKey(zpathToKey(event.getPath()));
        break;
      default:
        markDataStructureBroken("Unknown event type " + event + " on " + path);
      }
    } else {
      markDataStructureBroken("Bad event: " + event);
    }
  }
  public boolean waitForSynchronization(long timeout)
  throws InterruptedException {
    long endAt = System.currentTimeMillis() + timeout;
    synchronized (newDataArrived) {
      while (waitingForChildren || !waitingForData.isEmpty()) {
        checkError();
        long left = endAt - System.currentTimeMillis();
        if (left <= 0) return false;
        newDataArrived.wait(Math.min(100, left));
      }
    }
    return true;
  }
  
  private void checkError() {
    if (errorString != null) {
      throw new RuntimeException(errorString);
    }
  }
  
  private String zpathToKey(String zpath) {
    assert zpath.startsWith(this.path + "/");
    return zpath.replaceFirst(this.path + "/", "");
  }
  
  private String keyToZpath(String key) {
    return this.path + "/" + key;
  }
  
  private void updateAndWatchKey(String key) {
    LOG.info("[" + name + "] Going to watch key " + key);
    waitingForData.add(key);
    this.zk.getData(keyToZpath(key), this, new NodeDataCB(), null);
  }

  private void updateAndWatchChildren() {
    waitingForChildren = true;
    zk.getChildren(path, this, new ChildCallback(), null);
  }
  
  
  private void removeKey(String key) {
    synchronized (newDataArrived) {
      map.remove(key);
      waitingForData.remove(key);
      newDataArrived.notifyAll();
    }
  }



  public class ChildCallback implements ChildrenCallback {
    @Override
    public void processResult(int rc, String node, Object cbData,
      List<String> children) {
      LOG.info("[" + name + "] Got child callback for node " + node);
      Set<String> newChildren = new HashSet<String>(children);
      Set<String> oldChildren = map.keySet();
      
      Set<String> addedChildren = Sets.difference(newChildren, oldChildren);
      for (String child : addedChildren) {
        updateAndWatchKey(child);
      }
      synchronized (newDataArrived) {
        waitingForChildren = false;
        newDataArrived.notifyAll();
      }
    }
  }

  private class NodeDataCB implements DataCallback {
    @SuppressWarnings("deprecation")
    @Override
    public void processResult(int rc, String node, Object cbData,
        byte[] data, Stat stat) {
      synchronized (newDataArrived) {
  
        switch (rc) {
          case Code.Ok:
            String key = zpathToKey(node);
            LOG.info("[" + name + "] Got data for key: " + key);
            map.put(key, new Pair<byte[], Stat>(data, stat));
            break;
          case Code.NoNode:
            map.remove(node);
            break;
          default:
            markDataStructureBroken("Failed to get data for " +
                node + " rc=" + rc);
            break;
        }
        newDataArrived.notifyAll();
      }
    }

  }

  private void markDataStructureBroken(String err) {
    LOG.warn("[" + name + "] ZK Map broken because: " + err,
        new Exception("Trace info"));
    errorString = err;
  }



}
