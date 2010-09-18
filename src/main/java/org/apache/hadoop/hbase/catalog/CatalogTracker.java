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
package org.apache.hadoop.hbase.catalog;

import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.MetaNodeTracker;
import org.apache.hadoop.hbase.zookeeper.RootRegionTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Tracks the availability of the catalog tables <code>-ROOT-</code> and
 * <code>.META.</code>.
 * <p>
 * This class is "read-only" in that the locations of the catalog tables cannot
 * be explicitly set.  Instead, ZooKeeper is used to learn of the availability
 * and location of ROOT.  ROOT is used to learn of the location of META.  If not
 * available in ROOT, ZooKeeper is used to monitor for a new location of META.
 * <p>Call {@link #start()} to start up operation.
 */
public class CatalogTracker {
  private static final Log LOG = LogFactory.getLog(CatalogTracker.class);

  private final HConnection connection;

  private final ZooKeeperWatcher zookeeper;

  private final RootRegionTracker rootRegionTracker;

  private final MetaNodeTracker metaNodeTracker;

  private final AtomicBoolean metaAvailable = new AtomicBoolean(false);
  private HServerAddress metaLocation;

  private final long defaultTimeout;

  public static final byte [] ROOT_REGION =
    HRegionInfo.ROOT_REGIONINFO.getRegionName();

  public static final byte [] META_REGION =
    HRegionInfo.FIRST_META_REGIONINFO.getRegionName();

  public static final long TIMEOUT_FOREVER = 0;

  /**
   * Constructs the catalog tracker.  Find current state of catalog tables and
   * begin active tracking by executing {@link #start()}.
   * @param zk
   * @param connection server connection
   * @param abortable if fatal exception
   * @throws IOException 
   */
  public CatalogTracker(final ZooKeeperWatcher zk, final HConnection connection,
      final Abortable abortable)
  throws IOException {
    this(zk, connection, abortable, TIMEOUT_FOREVER);
  }

  /**
   * Constructs the catalog tracker.  Find current state of catalog tables and
   * begin active tracking by executing {@link #start()}.
   * @param zk
   * @param connection server connection
   * @param abortable if fatal exception
   * @param defaultTimeout Timeout to use.
   * @throws IOException 
   */
  public CatalogTracker(final ZooKeeperWatcher zk, final HConnection connection,
      final Abortable abortable, final long defaultTimeout)
  throws IOException {
    this.zookeeper = zk;
    this.connection = connection;
    this.rootRegionTracker = new RootRegionTracker(zookeeper, abortable);
    this.metaNodeTracker = new MetaNodeTracker(zookeeper, this);
    this.defaultTimeout = defaultTimeout;
  }

  /**
   * Starts the catalog tracker.
   * <p>
   * Determines current availability of catalog tables and ensures all further
   * transitions of either region is tracked.
   * @throws IOException
   * @throws InterruptedException 
   */
  public void start() throws IOException, InterruptedException {
    // Register listeners with zk
    zookeeper.registerListener(rootRegionTracker);
    zookeeper.registerListener(metaNodeTracker);
    // Start root tracking
    rootRegionTracker.start();
    // Determine meta assignment; may not work because root and meta not yet
    // deployed.
    getMetaServerConnection(true);
  }

  /**
   * Gets the current location for <code>-ROOT-</code> or null if location is
   * not currently available.
   * @return location of root, null if not available
   * @throws InterruptedException 
   */
  public HServerAddress getRootLocation() throws InterruptedException {
    return this.rootRegionTracker.getRootRegionLocation();
  }

  /**
   * @return Location of meta or null if not yet available.
   */
  public HServerAddress getMetaLocation() {
    return this.metaLocation;
  }

  /**
   * Waits indefinitely for availability of <code>-ROOT-</code>.  Used during
   * cluster startup.
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitForRoot()
  throws InterruptedException {
    rootRegionTracker.getRootRegionLocation();
  }

  /**
   * Gets the current location for <code>-ROOT-</code> if available and waits
   * for up to the specified timeout if not immediately available.  Returns null
   * if the timeout elapses before root is available.
   * @param timeout maximum time to wait for root availability, in milliseconds
   * @return location of root
   * @throws InterruptedException if interrupted while waiting
   * @throws NotAllMetaRegionsOnlineException if root not available before
   *                                          timeout
   */
  public HServerAddress waitForRoot(final long timeout)
  throws InterruptedException, NotAllMetaRegionsOnlineException {
    HServerAddress address = rootRegionTracker.waitRootRegionLocation(timeout);
    if (address == null) {
      throw new NotAllMetaRegionsOnlineException("Timed out; " + timeout + "ms");
    }
    return address;
  }

  /**
   * Gets a connection to the server hosting root, as reported by ZooKeeper,
   * waiting up to the specified timeout for availability.
   * @see #waitForRoot(long) for additional information
   * @return connection to server hosting root
   * @throws InterruptedException
   * @throws NotAllMetaRegionsOnlineException if timed out waiting
   * @throws IOException
   */
  public HRegionInterface waitForRootServerConnection(long timeout)
  throws InterruptedException, NotAllMetaRegionsOnlineException, IOException {
    return getCachedConnection(waitForRoot(timeout));
  }

  /**
   * Gets a connection to the server hosting root, as reported by ZooKeeper,
   * waiting for the default timeout specified on instantiation.
   * @see #waitForRoot(long) for additional information
   * @return connection to server hosting root
   * @throws NotAllMetaRegionsOnlineException if timed out waiting
   * @throws IOException
   */
  public HRegionInterface waitForRootServerConnectionDefault()
  throws NotAllMetaRegionsOnlineException, IOException {
    try {
      return getCachedConnection(waitForRoot(defaultTimeout));
    } catch (InterruptedException e) {
      throw new NotAllMetaRegionsOnlineException("Interrupted");
    }
  }

  /**
   * Gets a connection to the server hosting root, as reported by ZooKeeper,
   * if available.  Returns null if no location is immediately available.
   * @return connection to server hosting root, null if not available
   * @throws IOException
   * @throws InterruptedException 
   */
  private HRegionInterface getRootServerConnection()
  throws IOException, InterruptedException {
    HServerAddress address = rootRegionTracker.getRootRegionLocation();
    if (address == null) {
      return null;
    }
    return getCachedConnection(address);
  }

  /**
   * Gets a connection to the server currently hosting <code>.META.</code> or
   * null if location is not currently available.
   * <p>
   * If a location is known, a connection to the cached location is returned.
   * If refresh is true, the cached connection is verified first before
   * returning.  If the connection is not valid, it is reset and rechecked.
   * <p>
   * If no location for meta is currently known, method checks ROOT for a new
   * location, verifies META is currently there, and returns a cached connection
   * to the server hosting META.
   *
   * @return connection to server hosting meta, null if location not available
   * @throws IOException
   * @throws InterruptedException 
   */
  private HRegionInterface getMetaServerConnection(boolean refresh)
  throws IOException, InterruptedException {
    synchronized(metaAvailable) {
      if(metaAvailable.get()) {
        HRegionInterface current = getCachedConnection(metaLocation);
        if(!refresh) {
          return current;
        }
        if(verifyRegionLocation(current, META_REGION)) {
          return current;
        }
        resetMetaLocation();
      }
      HRegionInterface rootConnection = getRootServerConnection();
      if(rootConnection == null) {
        return null;
      }
      HServerAddress newLocation = MetaReader.readMetaLocation(rootConnection);
      if(newLocation == null) {
        return null;
      }
      HRegionInterface newConnection = getCachedConnection(newLocation);
      if(verifyRegionLocation(newConnection, META_REGION)) {
        setMetaLocation(newLocation);
        return newConnection;
      }
      return null;
    }
  }

  /**
   * Waits indefinitely for availability of <code>.META.</code>.  Used during
   * cluster startup.
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitForMeta() throws InterruptedException {
    synchronized(metaAvailable) {
      while(!metaAvailable.get()) {
        metaAvailable.wait();
      }
    }
  }

  /**
   * Gets the current location for <code>.META.</code> if available and waits
   * for up to the specified timeout if not immediately available.  Throws an
   * exception if timed out waiting.
   * @param timeout maximum time to wait for meta availability, in milliseconds
   * @return location of meta
   * @throws InterruptedException if interrupted while waiting
   * @throws IOException unexpected exception connecting to meta server
   * @throws NotAllMetaRegionsOnlineException if meta not available before
   *                                          timeout
   */
  public HServerAddress waitForMeta(long timeout)
  throws InterruptedException, IOException, NotAllMetaRegionsOnlineException {
    long stop = System.currentTimeMillis() + timeout;
    synchronized(metaAvailable) {
      while (!metaAvailable.get()) {
        if(getMetaServerConnection(true) != null) {
          return metaLocation;
        }
        if (timeout == TIMEOUT_FOREVER || System.currentTimeMillis() < stop) {
          if (timeout == TIMEOUT_FOREVER) {
            metaAvailable.wait(1000);
          } else {
            metaAvailable.wait(Math.min(1000, timeout));
          }
        } else {
          throw new NotAllMetaRegionsOnlineException(
            "Timed out (" + timeout + "ms)");
        }
      }
      return metaLocation;
    }
  }

  /**
   * Gets a connection to the server hosting meta, as reported by ZooKeeper,
   * waiting up to the specified timeout for availability.
   * @see #waitForMeta(long) for additional information
   * @return connection to server hosting meta
   * @throws InterruptedException
   * @throws NotAllMetaRegionsOnlineException if timed out waiting
   * @throws IOException
   */
  public HRegionInterface waitForMetaServerConnection(long timeout)
  throws InterruptedException, NotAllMetaRegionsOnlineException, IOException {
    return getCachedConnection(waitForMeta(timeout));
  }

  /**
   * Gets a connection to the server hosting meta, as reported by ZooKeeper,
   * waiting up to the specified timeout for availability.
   * @see #waitForMeta(long) for additional information
   * @return connection to server hosting meta
   * @throws NotAllMetaRegionsOnlineException if timed out or interrupted
   * @throws IOException
   */
  public HRegionInterface waitForMetaServerConnectionDefault()
  throws NotAllMetaRegionsOnlineException, IOException {
    try {
      return getCachedConnection(waitForMeta(defaultTimeout));
    } catch (InterruptedException e) {
      throw new NotAllMetaRegionsOnlineException("Interrupted");
    }
  }

  private void resetMetaLocation() {
    LOG.info("Current cached META location is not valid, resetting");
    metaAvailable.set(false);
    metaLocation = null;
  }

  private void setMetaLocation(HServerAddress metaLocation) {
    LOG.info("Found new META location, " + metaLocation);
    metaAvailable.set(true);
    this.metaLocation = metaLocation;
    // no synchronization because these are private and already under lock
    metaAvailable.notifyAll();
  }

  private HRegionInterface getCachedConnection(HServerAddress address)
  throws IOException {
    HRegionInterface protocol = null;
    try {
      protocol = connection.getHRegionConnection(address, false);
    } catch (RetriesExhaustedException e) {
      if (e.getCause() != null && e.getCause() instanceof ConnectException) {
        // Catch this; presume it means the cached connection has gone bad.
      } else {
        throw e;
      }
    } catch (IOException e) {
      if (e.toString().contains(
            "Server is not yet accepting RPCs")) {
        // TODO: make a new exception type like ServerNotReadyException
        // Also OK
      } else {
        throw e;
      }
    }
    return protocol;
  }

  private boolean verifyRegionLocation(HRegionInterface metaServer,
      byte [] regionName) {
    try {
      return metaServer.getRegionInfo(regionName) != null;
    } catch (NotServingRegionException e) {
      return false;
    } catch (IOException ioe) {
      if (ioe.toString().contains("Connection reset") ||
          ioe.toString().contains("not yet accepting RPCs")) {
        return false;
      }
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Check if <code>hsi</code> was carrying <code>-ROOT-</code> or
   * <code>.META.</code> and if so, clear out old locations.
   * @param hsi Server that has crashed/shutdown.
   * @throws InterruptedException
   * @throws KeeperException
   * @return Pair of booleans; if this server was carrying root, then first
   * boolean is set, if server was carrying meta, then second boolean set.
   */
  public Pair<Boolean, Boolean> processServerShutdown(final HServerInfo hsi)
  throws InterruptedException, KeeperException {
    Pair<Boolean, Boolean> result = new Pair<Boolean, Boolean>(false, false);
    HServerAddress rootHsa = getRootLocation();
    if (rootHsa == null) {
      LOG.info("-ROOT- is not assigned; continuing");
    } else if (hsi.getServerAddress().equals(rootHsa)) {
      result.setFirst(true);
      LOG.info(hsi.getServerName() + " carrying -ROOT-; deleting " +
        "-ROOT- location from meta");
      RootLocationEditor.deleteRootLocation(this.zookeeper);
    }
    HServerAddress metaHsa = getMetaLocation();
    if (metaHsa == null) {
      LOG.info(".META. is not assigned; continuing");
    } else if (hsi.getServerAddress().equals(metaHsa)) {
      LOG.info(hsi.getServerName() + " carrying .META.; unsetting " +
        ".META. location");
      result.setSecond(true);
      resetMetaLocation();
    }
    return result;
  }
}