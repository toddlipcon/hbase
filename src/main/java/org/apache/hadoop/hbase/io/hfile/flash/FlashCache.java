/**
 * Copyright 2012 The Apache Software Foundation
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
package org.apache.hadoop.hbase.io.hfile.flash;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheColumnFamilySummary;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.CacheableDeserializer;
import org.apache.hadoop.hbase.io.hfile.CacheableDeserializerFactory;
import org.apache.hadoop.hbase.io.hfile.flash.FlashAllocator.FlashAllocatorException;
import org.apache.hadoop.hbase.util.HasThread;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/* 
 * Still need to work out how to evict blocks by HFile, too. This will minimise GC
 * a lot on merges. Need to keep another hash (could this be a Java hash to
 * a list of buckets, or ???)
 */

/**
 * Doc here
 **/
public class FlashCache implements BlockCache, HeapSize {
    static final Log LOG = LogFactory.getLog(FlashCache.class);

    public class Entry {
        private long mOffset;
        public long offset() { return mOffset; }
        public void setOffset(long value) { mOffset=value; }

        private short mLengthIndex;
        public short lengthIndex() { return mLengthIndex; }
        public void setLengthIndex(short value) { mLengthIndex=value; }

        private byte mDeserialiserIndex;
        public byte deserialiserIndex() { return mDeserialiserIndex; }
        public void setDeserialiserIndex(byte value) { mDeserialiserIndex=value; }

        public void setLengthBytes(int len) {
            mLengthIndex=((short)mLengthMap.map(len));
        }

        public int lengthBytes() {
            return mLengthMap.unmap(mLengthIndex);
        }
        
        public int lengthBytes(UniqueIndexMap<Integer>map) { // used in deserialisation only
        	return map.unmap(mLengthIndex);
        }

        protected CacheableDeserializer<Cacheable>deserializerReference() {
            return CacheableDeserializerFactory.getDeserializer(mDeserialiserMap.unmap(mDeserialiserIndex));
        }
        
        protected void setDeserialiserReference(CacheableDeserializer<Cacheable>deserializer) {
            mDeserialiserIndex=((byte)mDeserialiserMap.map(deserializer.getDeserialiserIdentifier()));
        }
    }
    public Entry createEntry() { return new Entry(); }

    IOEngine mIO;

    // We don't have enough bits in SerialEntry to store all possible length combinations,
    // since the length is an arbitrary number of bytes. However it's very likely there are
    // a finite number of possible lengths; this map takes care of this.
    UniqueIndexMap<Integer> mLengthMap=new UniqueIndexMap<Integer>();
    UniqueIndexMap<Integer> mDeserialiserMap=new UniqueIndexMap<Integer>();

    final class RAMQueueEntry {
        private BlockCacheKey mKey;
        private Cacheable mData;

        public RAMQueueEntry(BlockCacheKey bck,Cacheable data) {
            mKey=bck;
            mData=data;
        }

        public Cacheable getData() { return mData; }
        public BlockCacheKey getKey() { return mKey; }

        public Entry writeToCache() throws FlashAllocator.CacheFullException,IOException {
            int len=mData.getSerializedLength();
            if(len==0) // This cacheable thing can't be seralised...
                return null;
            Entry fe=new Entry();
            fe.setDeserialiserReference(mData.getDeserializer());
            fe.setLengthBytes(len);
            long offset=mAllocator.allocateBlock(len);
            fe.setOffset(offset);
            ByteBuffer bb=ByteBuffer.allocate(len);
            mData.serialize(bb);
            mIO.write(bb,offset);
            return fe;
        }
    }
    
    private CacheStats mStats=new CacheStats();

    /* Eviction statistics array:
     *   Bit 7: Referenced bit.
     *   Bit 1-6: Archived reference bits, shifted right on each clock tick
     * Indexed by modulo hashcode, same as the underlying forward map if it's direct.
     */
    private byte mCacheReferenced[];
    private String mPersistencePath;
    private long mCacheFileSize;
    
    // For stuff on the list to be written, this is how we look it up as it's being written...
    private ConcurrentHashMap<BlockCacheKey,RAMQueueEntry>mRAMCache;
    private ForwardMap mFlashCache;

    // We make use of a reader/writer lock in an unconventional way. Because HBase is an
    // append-based system, we can stuff new things in the cache with only the read lock
    // held since we don't care about races there. Only when we're asked to remove things
    // from the cache - or do our own eviction - do we need to ensure consistency to
    // avoid partially read blocks. In these cases we grab the writer lock.
    private final ReentrantReadWriteLock mReaderWriterLock=new ReentrantReadWriteLock();
    private final Lock mReaderLock=mReaderWriterLock.readLock();
    private final Lock mWriterLock=mReaderWriterLock.writeLock();
    
    // Flag that states if the cache is enabled or not... We shut it off if there's an IO
    // error, so that Flash IO exceptions/errors don't bring down the HBase server.
    private volatile boolean mCacheEnabled;
    
    // We want to make sure the clock thread and an eviction thread paren't running the
    // clock at the same time - otherwise eviction priorities will be skewed.
    private final Lock mClockLock=new ReentrantLock();
    
    // Stuff to be spooled out by the writer threads. We have more than one writer, since
    // we need several threads going in order to keep the flash fully occupied.
    final int CLOCK_BITS_REFERENCED=0x20,CLOCK_BITS_MASK=0x3F,CLOCK_BITS_MAX=64;
    private ArrayList<BlockingQueue<RAMQueueEntry>>mWriterQueues=new ArrayList<BlockingQueue<RAMQueueEntry>>();
    final int DEFAULT_WRITER_THREADS=8, DEFAULT_WRITER_QUEUE_ITEMS=64;
    private Thread mWriterThreads[];

    // This thread does the updating of the clocks.
    private final int CLOCK_THREAD_PERIOD_SECS=5;
    
    private final ScheduledExecutorService mPeriodicThreadPool=Executors.newScheduledThreadPool(2,
        new ThreadFactoryBuilder().setNameFormat("Flash cache thread pool #%d").setDaemon(true).build());

    // File handle for the cache, and allocator for it
    private FlashAllocator mAllocator;

    // Try to free space. We run this periodically...
    private class ClockThread extends HasThread {
        ClockThread() {
            super();
            setDaemon(true);
        }
    	public void run() {
            runClock();
    	}
    }
    
    private void runClock() {
    	mClockLock.lock();
    	int referenced=0;
    	try {
            for(int i=0;i<mCacheReferenced.length;++i) {
            	// TODO
                if((mCacheReferenced[i]&CLOCK_BITS_REFERENCED)!=0)
                    ++referenced;
                mCacheReferenced[i]=(byte)((mCacheReferenced[i]&CLOCK_BITS_MASK)>>>1);
            }
            LOG.info("NACFLASH: Clock counted "+referenced+" referenced bits since last sweep");
    	} finally {
            mClockLock.unlock();
    	}
    }

    // Optimisation: Can skip entries in clockTickAndSort that don't need treatment.
    private void freeSpace() {
    	mWriterLock.lock();
    	try {
            FlashAllocator.IndexStatistics[] stats=mAllocator.getIndexStatistics();

            // It's possible multiple callers will call this method wanting it to free space.
            // So before we do any work, we need to make sure that we actually need to do
            // said work...
            //
            // For each bucket size, see if the usage is within bounds. If not, we need to
            // free up some stuff.
            ArrayList<Integer>fullBucketNumbers=new ArrayList<Integer>();
            for(int i=0;i<stats.length;++i) {
                // free space goal 12.5% per bucket, but we free some more extra (18%) just in case we have
                // a queue of threads waiting to enter here...
                long freeGoal=stats[i].totalCount()>>3;
                if(freeGoal<1) // Some of the initial big buckets may never get used.
                    freeGoal=1;
                if(stats[i].freeCount()<freeGoal)
                    fullBucketNumbers.add(i);
            }
            if(fullBucketNumbers.size()==0) {
            	LOG.info("Flash allocator shortcut! No need to free space!! :-)");
                return; // Someone else just ran us - or we had enough room to begin with
            }

            // OK. We have to do some eviction :(
            // Run the clock. This will both update referenced bits and give us back an
            // ordered list of stuff to ditch. If we're running out of space more often,
            // this will get done more often than the default clock thread does, which
            // is in fact exactly what we want... We sort entries first by bucket size,
            // secondly by priority. For optimisation we should do something different here
            // if dealing with LARGE caches but for small caches this should be OK...
            for(int i:fullBucketNumbers)
                LOG.info("Flash to free space in allocator; for bucket size "+stats[i].itemSize()+" used="+stats[i].usedCount()+" total="+stats[i].totalCount());
            mStats.evict();
            

            // Run the clock. This will both update referenced bits and give us back an
            // ordered list of stuff to ditch. If we're running out of space more often,
            // this will get done more often than the default clock thread does, which
            // is in fact exactly what we want... We sort entries first by bucket size,
            // secondly by priority. For optimisation we should do something different here
            // if dealing with LARGE caches but for small caches this should be OK...
            mStats.evict();
            Object[]indexes=new Object[FlashAllocator.getMaximumAllocationIndex()];
            for(int i=0;i<indexes.length;++i) {
                ForwardMap.ReferenceList[]entries=new ForwardMap.ReferenceList[CLOCK_BITS_MAX];
                for(int j=0;j<entries.length;++j)
                    entries[j]=mFlashCache.factoryReferenceList();
                indexes[i]=entries;
            }

            mClockLock.lock();
            try {
                FlashCache.Entry ent=new FlashCache.Entry();
                for(ForwardMap.Reference ref:mFlashCache) {
                    int hash=ref.getHashCode()&0x7FFFFFFF;
                    hash%=mCacheReferenced.length;
                    // unsigned arithmetic java aargh...
                    int iflags=(int)(mCacheReferenced[hash]&CLOCK_BITS_MASK);
                    mCacheReferenced[hash]=(byte)(iflags>>>1);
                    ref.getEntryTo(ent);
                    ForwardMap.ReferenceList[]ents=(ForwardMap.ReferenceList[])indexes[mAllocator.sizeIndexOfAllocation(ent.offset())];
                    ents[iflags].add(ref);
                }
            } finally {
                mClockLock.unlock();
            }

            // Eviction race condition:
            // See in map, load from Flash, oops - meanwhile it got fucked. With in memory
            // structures you don't get this race because the reference got from the hash
            // table is still valid i.e. read of the data isn't delayed till after. This is
            // why we have taken the writer lock here.
            //
            // Iterate for each bucket size that we need to do.
            // TODO: Optimisation: Don't add in clockTickAndSort if we don't need to -
            // pass in a bitmap to check against!
            for(int bucketNo:fullBucketNumbers) {
            	ForwardMap.ReferenceList[]entries=(ForwardMap.ReferenceList[])indexes[bucketNo];
                long goal=stats[bucketNo].totalCount(),free=stats[bucketNo].freeCount();
                goal>>=3;
                goal+=(goal>>1);
                if(goal<2)
                    goal=2;
                assert 2<=FlashAllocator.LEAST_ITEMS_IN_BUCKET;
                LOG.info("...Flash clock executed; free entry goal for bucket size "+stats[bucketNo].itemSize()+" is "+goal);
                for(int i=0;i<entries.length;++i) {
                    ForwardMap.ReferenceList afl=entries[i];
                    LOG.info("   --> Priority group "+i+" contains "+afl.size()+" entries");
                }
                FlashCache.Entry ent=new FlashCache.Entry();
                while(free<goal) {
                    for(int i=0;i<entries.length;++i) {
                        ForwardMap.ReferenceList afl=entries[i];
                        for(ForwardMap.Reference me:afl) {
                            /* What I'd like to be able to do is TRIM these blocks too. Will help the
                             * drive go faster but can't work out how to from Java */
                            me.getEntryTo(ent);
                            mAllocator.freeBlock(ent.offset());
                            ++free;
                            mFlashCache.remove(me);
                            mCacheReferenced[(me.getHashCode()&0x7FFFFFFF)%mCacheReferenced.length]=0;
                            if(free>=goal)
                                break;
                        }
                    }
                }
            }
            assert shouldntHaveToFreeAnythingMore();
            LOG.info("Successfully freed space!!! Info follows:\n"+mAllocator.getInfo());
            mAllocator.logStatistics();
    	} finally {
            mWriterLock.unlock();
    	}
    }

    // Test method - re-runs the count of all the buckets in the allocator to make sure that
    // we hit our free space goals.
    boolean shouldntHaveToFreeAnythingMore() {
        FlashAllocator.IndexStatistics[] stats=mAllocator.getIndexStatistics();
        for(int i=0;i<stats.length;++i) {
            long freeGoal=stats[i].totalCount()>>3;
            if(freeGoal<1) // Some of the initial big buckets may never get used.
                freeGoal=1;
            if(stats[i].freeCount()<freeGoal)
                return false;
        }
        return true;
    }

    // This handles flushing the RAM cache to Flash.
    public class WriterThread extends HasThread {
        BlockingQueue<RAMQueueEntry>mInputQueue;
        int mThreadNo;
        WriterThread(BlockingQueue<RAMQueueEntry>q, int threadNo) {
            super();
            mInputQueue=q;
            mThreadNo=threadNo;
            setDaemon(true);
        }
        
        public void run() {
            List<RAMQueueEntry>entries=new ArrayList<RAMQueueEntry>();
            while(mCacheEnabled) {
            	try {
                    entries.add(mInputQueue.take()); // Blocks
                    mInputQueue.drainTo(entries); // Add any others remaining. Does not block.
            	} catch(InterruptedException ie) {
                    /* If we're interrupted and we're done, we might lose a few blocks here.
                     * No problem.
                     */
                    if(!mCacheEnabled)
                        return;
            	}
                doDrain(entries);
            }
        }

        private void doDrain(List<RAMQueueEntry> entries) {
            Entry[]flashEntries=new Entry[entries.size()];
            RAMQueueEntry[]ramEntries=new RAMQueueEntry[entries.size()];
            int done=0;
            long written=0;
            while(entries.size()>0) { // Keep going in case we throw...
                try {
                    RAMQueueEntry e=entries.get(entries.size()-1);
                    Entry fe=e.writeToCache();
                    written+=fe.lengthBytes();
                    ramEntries[done]=e;
                    flashEntries[done++]=fe;
                    entries.remove(entries.size()-1);
                } catch(FlashAllocator.CacheFullException cfe) {
                    LOG.info("Out of room in flash cache; freeing space... ");
                    freeSpace();
                } catch(IOException ioex) {
                    LOG.error("IO exception writing to flash cache; disabling... check your SSD or ioDrive is in good health...",ioex);
                    disableCache();
                    return;
                }
            }
            
            // Make sure that the data pages we written are on the media before we update the forward map.
            try {
            	mIO.sync();
            } catch(IOException ioex) {
                LOG.error("IO exception writing to flash cache; disabling... check your SSD or ioDrive is in good health...",ioex);
                disableCache();
                return;
            }

            // The loop is split in two so we're not holding the writer lock while
            // waiting for the ioDrive to write...
            // Ordering: Put in RAM cache, put in drain queue, write to Flash, add into flash cache, remove from RAM cache
            // Eviction: Snapshot flash cache. Run through; remove from Flash Cache, then de-allocate, less-used things.
            // Issue is I might have been deallocated in-between writing to Flash and adding to FlashCache? Can't be,
            // because if I'm not in flash cache in the first place I'd never been scheduled for eviction. Bingo!
            mWriterLock.lock();

            // Make sure that we don't have dups. TODO DEBUG.
            try {
                for(int i=0;i<done;++i) {
                    if(flashEntries[i]!=null)
                        mFlashCache.put(ramEntries[i].getKey(), flashEntries[i]);
                }
            } finally {
                mWriterLock.unlock();
            }
            for(int i=0;i<done;++i)
                mRAMCache.remove(ramEntries[i].getKey());
        }
    }

    public FlashCache(String filePath,long fileSize,int writerThreads,int writerQLen,
    		String persistencePath) throws java.io.FileNotFoundException,IOException {    	
    	boolean useDirectHash=false;
    	String msg="";
    	if(filePath.startsWith("directHash:")) {
    		useDirectHash=true;
    		filePath=filePath.substring("directHash:".length());
    	}
    	if(filePath.startsWith("file:"))
            mIO=new FlashIOEngine(filePath.substring(5),fileSize);
    	else if(filePath.startsWith("offHeap:"))
            mIO=new DirectArrayIOEngine(filePath.substring(8),fileSize,true);
    	else if(filePath.startsWith("heap:"))
            mIO=new DirectArrayIOEngine(filePath.substring(5),fileSize,false);
    	else
            throw new IOException("Don't understand file path for cache - prefix with file:, heap: or offHeap:");

        // TODO: Implement me...
        //        this.scheduleThreadPool.scheduleAtFixedRate(new StatisticsThread(this),
        //            STAT_THREAD_PERIOD_SECS, STAT_THREAD_PERIOD_SECS, TimeUnit.SECONDS);

        mWriterThreads=new Thread[writerThreads];
        long hashSize=DirectHashForwardMap.recommendedHashSize(fileSize/16384);
        assert hashSize<Integer.MAX_VALUE; // Enough for about 32TB of cache! Enough...
        mCacheFileSize=fileSize;
        mPersistencePath=persistencePath;
        mCacheReferenced=new byte[(int)hashSize];

        try {
            mAllocator=new FlashAllocator(fileSize);
        } catch(FlashAllocator.FlashAllocatorException fex) {
            throw new IOException("File size too small");
        }
        for(int i=0;i<mWriterThreads.length;++i)
            mWriterQueues.add(new ArrayBlockingQueue<RAMQueueEntry>(writerQLen));
        assert mWriterQueues.size()==mWriterThreads.length;
        mRAMCache=new ConcurrentHashMap<BlockCacheKey,RAMQueueEntry>();
        if(useDirectHash) {
            mFlashCache=new DirectHashForwardMap((int)(fileSize/16384),false);
            msg+="...Using direct off-heap hash table for cache meta-data";
        } else {
        	mFlashCache=new HeapForwardMap((int)(fileSize/16384));
            msg+="...Using inbuilt Map for cache meta-data";
        }

        if(mIO.isPersistent()&&mPersistencePath!=null) {
        	try {
        		retrieveFromFile();
        	} catch(IOException ioex) {
        		LOG.error("Flash cache: Can't restore from file because of",ioex);
        	} catch(FlashAllocator.FlashAllocatorException faex) {
        		LOG.error("Flash cache: Can't restore from file in rebuild because of",faex);
        	} catch(ClassNotFoundException cnfe) {
        		LOG.error("Flash cache: Can't restore from file in rebuild because can't deserialise",cnfe);
        	}
        }

        mPeriodicThreadPool.scheduleAtFixedRate(new ClockThread(),CLOCK_THREAD_PERIOD_SECS,
                CLOCK_THREAD_PERIOD_SECS,TimeUnit.SECONDS);
        for(int i=0;i<mWriterThreads.length;++i) {
            mWriterThreads[i]=new Thread(new WriterThread(mWriterQueues.get(i),i));
            mWriterThreads[i].setName("Flash cache writer "+i);
        }
        mCacheEnabled=true;
        for(int i=0;i<mWriterThreads.length;++i) {
            mWriterThreads[i].start();
        }
        LOG.info("NACFLASH: Started flash cache");
    }
    
    private void dumpToLog() {
    	StringBuilder buff=new StringBuilder();
    	FlashCache.Entry entry=new FlashCache.Entry();
    	ArrayList<String>entries=new ArrayList<String>();
    	for(ForwardMap.Reference ref:mFlashCache) {
    		ref.getEntryTo(entry);
    		entries.add(ref.getHashCode()+":"+entry.offset());
    	}
    	String[]entriesarr=new String[entries.size()];
    	entriesarr=(String[])entries.toArray(entriesarr);
    	Arrays.sort(entriesarr);
    	for(String s:entriesarr)
    		buff.append(s).append(',');
    	LOG.info(buff);
    	mAllocator.dumpToLog();
    }

    private void persistToFile() throws IOException {
    	// Grabbing the writer lock here guarantees that the flash cache is persistent. There
    	// may be stuff in RAM cache that's not fully drained into it - in which case, it
    	// just gets tossed, oh well...
    	mWriterLock.lock();
    	assert !mCacheEnabled;
    	FileOutputStream fos=null;
    	ObjectOutputStream oos=null;
    	try {
	    	if(!mIO.isPersistent())
	    		throw new IOException("Attempt to persist non-persistent cache mappings!");
	    	fos=new FileOutputStream(mPersistencePath,false);
	    	oos=new ObjectOutputStream(fos);
	    	oos.writeLong(mCacheFileSize);
	    	oos.writeInt(mCacheReferenced.length);
	    	oos.writeUTF(mIO.getClass().getName());
	    	oos.writeUTF(mFlashCache.getClass().getName());
	    	oos.writeObject(mCacheReferenced);
	    	oos.writeObject(mLengthMap);
	    	oos.writeObject(mDeserialiserMap);
	    	mFlashCache.persistTo(oos);
    	} finally {
    		if(oos!=null)
    			oos.close();
    		if(fos!=null)
    			fos.close();
    		mWriterLock.unlock();
    		//dumpToLog();
    		try {
    			FlashAllocator fa2=new FlashAllocator(mCacheFileSize,this,mFlashCache,mLengthMap);
    			mAllocator.dumpToLog();
    			fa2.dumpToLog();
    		} catch(FlashAllocatorException fex) {}
    	}
    }
    
    private void retrieveFromFile() throws IOException,FlashAllocator.FlashAllocatorException,ClassNotFoundException {
    	mWriterLock.lock();
    	assert !mCacheEnabled;
    	FileInputStream fis=null;
    	ObjectInputStream ois=null;
    	try {
	    	if(!mIO.isPersistent())
	    		throw new IOException("Attempt to restore non-persistent cache mappings!");
	    	fis=new FileInputStream(mPersistencePath);
	    	ois=new ObjectInputStream(fis);
	    	long cacheFileSize=ois.readLong();
	    	int cacheRefLen=ois.readInt();
	    	if(cacheFileSize!=mCacheFileSize||cacheRefLen!=mCacheReferenced.length)
	    		throw new IOException("Can't restore Flash cache because of mismatched cache size");
	    	String ioclass=ois.readUTF();
	    	String mapclass=ois.readUTF();
	    	if(!mIO.getClass().getName().equals(ioclass))
	    		throw new IOException("Class name for IO engine mismatch: "+ioclass);
	    	if(!mFlashCache.getClass().getName().equals(mapclass))
	    		throw new IOException("Class name for cache map mismatch: "+mapclass);
	    	byte[]cacheReferenced=(byte[])ois.readObject();
	    	UniqueIndexMap<Integer>lengthMap=(UniqueIndexMap<Integer>)ois.readObject();
	    	UniqueIndexMap<Integer>deserMap=(UniqueIndexMap<Integer>)ois.readObject();
	    	mFlashCache.retrieveFrom(ois);
	    	FlashAllocator fa=new FlashAllocator(mCacheFileSize,this,mFlashCache,lengthMap);
	    	mAllocator=fa;
	    	mCacheReferenced=cacheReferenced;
	    	mLengthMap=lengthMap;
	    	mDeserialiserMap=deserMap;
	    } finally {
    		if(ois!=null)
    			ois.close();
    		if(fis!=null)
    			fis.close();
    		mWriterLock.unlock();
    	}
    }

    // Used to shut down the cache -or- turn it off in the case of something broken.
    private void disableCache() {
    	if(!mCacheEnabled)
            return;
    	mCacheEnabled=false;
    	if(mIO!=null)
            mIO.shutdown();
    	this.mPeriodicThreadPool.shutdown();
        for(int i=0;i<mWriterThreads.length;++i)
            mWriterThreads[i].interrupt(); // This will cause the thread to unblock and check mCacheEnabled.
    }

    public void shutdown() {
    	disableCache();
    	LOG.info("Flash: IO persistent="+mIO.isPersistent()+"; path to write="+mPersistencePath);
        if(mIO.isPersistent()&&mPersistencePath!=null) {
        	try {
        		persistToFile();
        	} catch(IOException ex) {
        		LOG.error("Flash: Unable to persist data on exit: "+ex.toString(),ex);
        	}
        }
    }

    public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory) {
        cacheBlock(cacheKey, buf);
    }

    // No locking needed here. Nothing can get in the way of putting stuff into
    // the RAM cache. This is the goal because we want this to return as fast as
    // possible.
    public void cacheBlock(BlockCacheKey key, Cacheable cachedItem) {
    	if(!mCacheEnabled)
            return;

        /* We can check for thrashing here, or in the loop where we drain the RAM cache.
         * Here is good enough and easier.
         */
        if(mFlashCache.containsKey(key))
            return;

        /* Stuff the entry into the RAM cache so it can get drained to the persistent store
         */
        RAMQueueEntry re=new RAMQueueEntry(key,cachedItem);
    	mRAMCache.put(key,re);
    	BlockingQueue<RAMQueueEntry>bq=mWriterQueues.get((key.hashCode()&0x7FFFFFFF)%mWriterQueues.size());
        if(!bq.offer(re)) {
            // Must add to queue after putting in RAM cache to avoid a (highly unlikely) race where we
            // get written to Flash before being inserted in RAM cache, and end up leaking RAM cache memory
            LOG.info("Failed to add key"+key.toString()+"to flash writer queue; queue backing up; skipping");
            mRAMCache.remove(key);
        }
    } 

    // Take the reader lock. This is in case stuff needs to be flushed out while
    // we're half-way through getting things. We only lock the flash cache since
    // the RAM cache can't get us into trouble since - if the get succeeds -
    // we know we still have the data alongside :)
    public Cacheable getBlock(BlockCacheKey key, boolean caching) {
    	if(!mCacheEnabled)
            return null;
    	RAMQueueEntry re=mRAMCache.get(key);
        if(re!=null) {
            mStats.hit(caching);
            return re.getData();
        }
    	try {
            mReaderLock.lock();
            Entry fe=new Entry();
            if(mFlashCache.get(key,fe)) {
                mStats.hit(caching);
                mCacheReferenced[key.fcHashCode(mCacheReferenced.length)]|=CLOCK_BITS_REFERENCED;
                // TODO Debugging
                int len=fe.lengthBytes();
                ByteBuffer bb=ByteBuffer.allocate(len);
                mIO.read(bb,fe.offset());
                return fe.deserializerReference().deserialize(bb);
            }
    	} catch(IOException ioex) {
            LOG.error("IO exception reading from flash cache; disabling... check your SSD or ioDrive is in good health..."+ioex.toString(),ioex);
            disableCache();
    	} finally {
            mReaderLock.unlock();
    	}
    	mStats.miss(caching);
        return null;
    }

    // Eviction can stuff the flash cache, but not the RAM cache. So take the
    // writer lock on eviction.
    public boolean evictBlock(BlockCacheKey cacheKey) {
    	mRAMCache.remove(cacheKey);
    	mWriterLock.lock();
    	try {
            Entry fe=new Entry();
            if(mFlashCache.get(cacheKey,fe)) {
            	mFlashCache.remove(cacheKey);
            	mAllocator.freeBlock(fe.offset());
                mCacheReferenced[cacheKey.fcHashCode(mCacheReferenced.length)]=0;
            }
            mStats.evicted();
    	} finally {
            mWriterLock.unlock();
    	}
        return true;
    }

    public CacheStats getStats() {
    	return mStats;
    }

    // TODO: BlockSize shite
    public long heapSize() {
        return 0;
    }

    public long size() {
        return 0;
    }

    public long getFreeSize() {
        return 0; // this cache, by default, allocates all its space.
    }

    public long getBlockCount() {
        return 0;
    }

    public long getCurrentSize() {
        return 0;
    }

    public long getEvictedCount() {
        return mStats.getEvictedCount();
    }

    /* This is a good optimisation. We could do this by keeping some extra lists of
     * hashCodes by hFileName. Because we use hashcodes they can be stuck in dumb
     * arrays. We could also TRIM the underlying storage device too. However it's not
     * necessary so it's left as an optimisation for later.
     */
    public int evictBlocksByHfileName(String hfileName) {
        return 0;
    }

    /* Does not need to be necessary. */
    public List<BlockCacheColumnFamilySummary> getBlockCacheColumnFamilySummaries(
        Configuration conf) {
        throw new UnsupportedOperationException();
    }
}
