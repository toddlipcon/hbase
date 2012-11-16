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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// Concrete plans:
//
//   Freeing space: We need to get the allocator to split the prioritised allocations
//   by bucket size, to make sure that enough free space exists per bucket size.
//
//   Missing bucket: Track allocation requests by bucket size in the last clock
//   cycle. If super-out-of-balance, axe some random other buckets to rebalance.
//   This should allow only a max axe of 1 bucket per cycle, to avoid too much
//   churn --- maybe? Or just fuck it?

public final class FlashAllocator {
    static final Log LOG = LogFactory.getLog(FlashCache.class);
    public class CacheFullException extends Exception {
    	private int mRequestedSize,mBucketIndex;
    	CacheFullException(int requestedSize,int bucketIndex) {
            super();
            mRequestedSize=requestedSize;
            mBucketIndex=bucketIndex;
    	}
        public int bucketIndex() { return mBucketIndex; }
        public int requestedSize() { return mRequestedSize; }
        public String info() {
            StringBuilder sb=new StringBuilder(1024);
            sb.append("Allocator requested size ").append(mRequestedSize);
            sb.append(" for bucket ").append(mBucketIndex);
            sb.append("; allocator info follows:  \n");
            sb.append(getInfo());
            return sb.toString();
        }
    }

    public class FlashAllocatorException extends Exception {
    	FlashAllocatorException(String reason) {
            super(reason);
    	}
    }

    final private class Bucket {
        private long mBaseOffset;
        private int mSize,mSizeIndex;
        private int mItemCount;
        private int mFreeList[];
        private int mFreeCount,mUsedCount;

        public Bucket(long offset) {
            mBaseOffset=offset;
            mSizeIndex=-1;
        }

        void reconfigure(int sizeIndex) {
            mSizeIndex=sizeIndex;
            mSize=BUCKET_SIZES[mSizeIndex];
            mItemCount=(int)(((long)BUCKET_SIZE)/(long)mSize);
            mFreeCount=mItemCount;
            mUsedCount=0;
            mFreeList=new int[mItemCount];
            for(int i=0;i<mFreeCount;++i)
                mFreeList[i]=i;
        }

        public boolean isUninstantiated() { return mSizeIndex==-1; }
        public int sizeIndex() { return mSizeIndex; }
        public int size() { return mSize; }
        public boolean isFree() { return mFreeCount>0; }
        public boolean isCompletelyFree() { return mUsedCount==0; }
        public int freeCount() { return mFreeCount; }
        public int usedCount() { return mUsedCount; }
        public int freeBytes() { return mFreeCount*mSize; }
        public int usedBytes() { return mUsedCount*mSize; }
        public long baseOffset() { return mBaseOffset; }

        public long allocate() {
            assert mFreeCount>0; // Else should not have been called
            assert mSizeIndex!=-1;
            ++mUsedCount;
            return mBaseOffset+(mFreeList[--mFreeCount]*mSize);
        }
        
        public void addAllocation(long offset) throws FlashAllocatorException {
            offset-=mBaseOffset;
            if(offset<0||offset%mSize!=0)
                throw new FlashAllocatorException("Attempt to add allocation for bad offset: "+offset+" base="+mBaseOffset);
            int idx=(int)(offset/mSize);
            boolean copyDown=false;
            for(int i=0;i<mFreeList.length;++i) {
                if(copyDown)
                    mFreeList[i-1]=mFreeList[i];
                else if(mFreeList[i]==idx)
                    copyDown=true;
            }
            if(!copyDown)
                throw new FlashAllocatorException("Couldn't find match for index "+idx+" in free list");
            ++mUsedCount;
            --mFreeCount;
        }

        private void free(long offset) {
            offset-=mBaseOffset;
            assert offset>=0;
            assert offset<mItemCount*mSize;
            assert offset%mSize==0;
            assert mUsedCount>0;
            assert mFreeCount<mItemCount; // Else duplicate free
            int item=(int)(offset/(long)mSize);
            assert !freeListContains(item);
            --mUsedCount;
            mFreeList[mFreeCount++]=item;
        }

        private boolean freeListContains(int blockNo) {
            for(int i=0;i<mFreeCount;++i)
                assert mFreeList[i]!=blockNo;
            return false;
        }
    }

    final private class BucketSizeInfo {
        private List<Bucket>mBuckets,mFreeBuckets,mCompletelyFreeBuckets;
        private int mSizeIndex;

        BucketSizeInfo(int sizeIndex) {
            mBuckets=new ArrayList<Bucket>();
            mFreeBuckets=new ArrayList<Bucket>();
            mCompletelyFreeBuckets=new ArrayList<Bucket>();
            mSizeIndex=sizeIndex;
        }

        public void instantiateBucket(Bucket b) {
            assert b.isUninstantiated()||b.isCompletelyFree();
            b.reconfigure(mSizeIndex);
            mBuckets.add(b);
            mFreeBuckets.add(b);
            mCompletelyFreeBuckets.add(b);
        }

        public int sizeIndex() { return mSizeIndex; }

        public long allocateBlock() {
            Bucket b=null;
            if(mFreeBuckets.size()>0) // Use up an existing one first...
                b=mFreeBuckets.get(mFreeBuckets.size()-1);
            if(b==null) {
                b=grabGlobalCompletelyFreeBucket();
                if(b!=null)
                    instantiateBucket(b);
            }
            if(b==null)
                return -1;
            long result=b.allocate();
            blockAllocated(b);
            return result;
        }
        
        void blockAllocated(Bucket b) {
            if(!b.isCompletelyFree())
                mCompletelyFreeBuckets.remove(b);
            if(!b.isFree())
                mFreeBuckets.remove(b);
        }

        public Bucket findAndRemoveCompletelyFreeBucket() {
            Bucket b=null;
            assert mBuckets.size()>0;
            if(mBuckets.size()==1) // So we never get complete starvation of a bucket for a size
            	return null;
            if(mCompletelyFreeBuckets.size()>0) {
                b=mCompletelyFreeBuckets.get(0);
                removeBucket(b);
            }
            return b;
        }

        private void removeBucket(Bucket b) {
            assert b.isCompletelyFree();
            mBuckets.remove(b);
            mFreeBuckets.remove(b);
            mCompletelyFreeBuckets.remove(b);
        }

        public void freeBlock(Bucket b,long offset) {
            assert mBuckets.contains(b);
            assert(!mCompletelyFreeBuckets.contains(b)); // else we shouldn't have anything to free...
            b.free(offset);
            if(!mFreeBuckets.contains(b))
                mFreeBuckets.add(b);
            if(b.isCompletelyFree())
                mCompletelyFreeBuckets.add(b);
        }

        public IndexStatistics statistics() {
            long free=0,used=0;
            for(Bucket b:mBuckets) {
                free+=b.freeCount();
                used+=b.usedCount();
            }
            return new IndexStatistics(free,used,BUCKET_SIZES[mSizeIndex]);
        }
    }

    private static final int BUCKET_SIZES[]={
        4*1024+1024,8*1024+1024,16*1024+1024,32*1024+1024,
        48*1024+1024,64*1024+1024,96*1024+1024,128*1024+1024,
        192*1024+1024,256*1024+1024,384*1024+1024,512*1024+1024
        ,1024*1024+1024};
    private Integer BUCKET_SIZE_INTEGERS[];

    public BucketSizeInfo roundUpToBucketSizeInfo(int osz) {
        for(int i=0;i<BUCKET_SIZES.length;++i)
            if(osz<=BUCKET_SIZES[i])
                return mBucketsBySize[i];
        return null;
    }

    static final int BIG_ITEM_SIZE=(1024*1024)+1024; // 1MB plus overhead
    static final Integer BIG_ITEM_SIZE_INTEGER=new Integer(BIG_ITEM_SIZE);
    static public final int LEAST_ITEMS_IN_BUCKET=2;
    static final int BUCKET_SIZE=LEAST_ITEMS_IN_BUCKET*BIG_ITEM_SIZE;

    private Bucket mBuckets[];
    private BucketSizeInfo[]mBucketsBySize;

    FlashAllocator(long availableSpace) throws FlashAllocatorException {
        mBuckets=new Bucket[(int)(availableSpace/(long)BUCKET_SIZE)];
        if(mBuckets.length<BUCKET_SIZES.length)
            throw new FlashAllocatorException("Flash allocator size too small - must have room for at least "+BUCKET_SIZES.length+" buckets");
        mBucketsBySize=new BucketSizeInfo[BUCKET_SIZES.length];
        BUCKET_SIZE_INTEGERS=new Integer[BUCKET_SIZES.length];
        for(int i=0;i<BUCKET_SIZES.length;++i) {
            BUCKET_SIZE_INTEGERS[i]=new Integer(BUCKET_SIZES[i]);
            BucketSizeInfo bsi=new BucketSizeInfo(i);
            mBucketsBySize[i]=bsi;
        }
        for(int i=0;i<mBuckets.length;++i) {
            mBuckets[i]=new Bucket(i*BUCKET_SIZE);
            mBucketsBySize[i<BUCKET_SIZES.length?i:BUCKET_SIZES.length-1].instantiateBucket(mBuckets[i]);
        }
    }
    
    /* Rebuild the allocator's data structures from a persisted forward map. This
     * is a bit of a pig:
     *   First pass - reverse engineer bucket sizes and offsets from persisted data.
     *   We also validate the map here to make sure allocations are grouped as
     *   we would expect. We then set all buckets as free...
     *   Second pass - add the allocations to said buckets
     */
    FlashAllocator(long availableSpace,FlashCache fc,ForwardMap map,UniqueIndexMap<Integer>lengthMap) throws FlashAllocatorException {
    	this(availableSpace);

    	// each bucket has an offset, sizeindex. probably the buckets are too big
    	// in our default state. so what we do is reconfigure them according to what
    	// we've found. we can only reconfigure each bucket once; if more than once,
    	// we know there's a bug, so we just log the info, throw, and start again...
    	boolean[]reconfigured=new boolean[mBuckets.length];
    	FlashCache.Entry ie=fc.createEntry();
    	for(ForwardMap.Reference r:map) {
            r.getEntryTo(ie);
            long foundOff=ie.offset();
            int foundLen=ie.lengthBytes(lengthMap);
            int needBucketSizeIndex=-1;
            for(int i=0;i<BUCKET_SIZES.length;++i) {
                if(foundLen<=BUCKET_SIZES[i]) {
                    needBucketSizeIndex=i;
                    break;
                }
            }
            if(needBucketSizeIndex==-1)
            	throw new FlashAllocatorException("Can't match bucket size "+foundLen+"; clearing allocator");
            int foundBucketNo=(int)(foundOff/(long)BUCKET_SIZE);
            if(foundBucketNo<0||foundBucketNo>=mBuckets.length)
            	throw new FlashAllocatorException("Can't find bucket "+foundBucketNo+"; did you shrink the cache? Clearing allocator");
            Bucket b=mBuckets[foundBucketNo];
            if(reconfigured[foundBucketNo]==true) {
            	if(b.sizeIndex()!=needBucketSizeIndex)
                    throw new FlashAllocatorException("Inconsistent allocation to bucket map; clearing allocator");
            } else {
            	if(!b.isCompletelyFree())
                    throw new FlashAllocatorException("Reconfiguring bucket "+foundBucketNo+" but it's already allocated; corrupt data");
                // Need to remove the bucket from whichever list it's currently in at the moment...
            	BucketSizeInfo bsi=mBucketsBySize[needBucketSizeIndex];
                BucketSizeInfo oldbsi=mBucketsBySize[b.sizeIndex()];
                oldbsi.removeBucket(b);
            	bsi.instantiateBucket(b);
            	reconfigured[foundBucketNo]=true;
            }
            mBuckets[foundBucketNo].addAllocation(foundOff);
            mBucketsBySize[needBucketSizeIndex].blockAllocated(b);
    	}
    }

    public String getInfo() {
    	StringBuilder sb=new StringBuilder(1024);
        for(int i=0;i<mBuckets.length;++i) {
            Bucket b=mBuckets[i];
            sb.append("    Bucket ").append(i).append(": ").append(b.size());
            sb.append(" freeCount=").append(b.freeCount()).append(" used=").append(b.usedCount());
            sb.append('\n');
        }
        return sb.toString();
    }

    // Allocate a block. Returns -1 if under pressure.
    public synchronized long allocateBlock(int osz) throws CacheFullException {
        assert osz>0;
        BucketSizeInfo bsi=roundUpToBucketSizeInfo(osz);
        if(bsi==null)
            throw new IllegalArgumentException("Allocation size too big");
        long offset=bsi.allocateBlock();

        // TODO: What if we need to trash a whole bucket? How to handle this? Maybe better
        // to set block sizes and ratios ahead of time in config? Or just blow away cache
        // chunks - how to do this, especially without synchronisation?
        // Answer - return a list of shit that the parent can take care of freeing to make
        // the allocation. This takes care of the need to rebalance problem as well - just
        // be random!
        if(offset==-1)
            throw new CacheFullException(osz,bsi.sizeIndex()); // Ask caller to free up space and try again!
        return offset;
    }

    private Bucket grabGlobalCompletelyFreeBucket() {
        for(BucketSizeInfo bsi:mBucketsBySize) {
            Bucket b=bsi.findAndRemoveCompletelyFreeBucket();
            if(b!=null)
                return b;
        }
        return null;
    }

    public synchronized int freeBlock(long freeBlock) {
        int bucketNo=(int)(freeBlock/(long)BUCKET_SIZE);
        assert bucketNo>=0&&bucketNo<mBuckets.length;
        Bucket targetBucket=mBuckets[bucketNo];
        mBucketsBySize[targetBucket.sizeIndex()].freeBlock(targetBucket,freeBlock);
        return targetBucket.size();
    }
    
    public int sizeIndexOfAllocation(long offset) {
        int bucketNo=(int)(offset/(long)BUCKET_SIZE);
        assert bucketNo>=0&&bucketNo<mBuckets.length;
        Bucket targetBucket=mBuckets[bucketNo];
        return targetBucket.sizeIndex();
    }

    static public int getMaximumAllocationIndex() {
        return BUCKET_SIZES.length;
    }
    
    public class IndexStatistics {
    	private long mFree,mUsed,mItemSize,mTotal;
    	public long freeCount() { return mFree; }
    	public long usedCount() { return mUsed; }
    	public long totalCount() { return mTotal; }
        public long freeBytes() { return mFree*mItemSize; }
        public long usedBytes() { return mUsed*mItemSize; }
        public long totalBytes() { return mTotal*mItemSize; }
        public long itemSize() { return mItemSize; }
    	public IndexStatistics(long free,long used,long itemSize) {
            setTo(free,used,itemSize);
    	}
    	public IndexStatistics() {
            setTo(-1,-1,0);
    	}
        public void setTo(long free,long used,long itemSize) {
            mItemSize=itemSize;
            mFree=free;
            mUsed=used;
            mTotal=free+used;
        }
    }

    public void dumpToLog() {
    	logStatistics();
    	StringBuilder sb=new StringBuilder();
    	for(Bucket b:mBuckets) {
            sb.append("Bucket:").append(b.mBaseOffset).append('\n');
            sb.append("  Size index: "+b.sizeIndex()+"; Free:"+b.mFreeCount+"; used:"+b.mUsedCount+"; freelist\n");
            for(int i=0;i<b.freeCount();++i)
                sb.append(b.mFreeList[i]).append(',');
    	}
    	LOG.info(sb);
    }

    public void logStatistics() {
        IndexStatistics total=new IndexStatistics();
        IndexStatistics[]stats=getIndexStatistics(total);
        LOG.info("Flash allocator statistics follow:\n");
        LOG.info("  Free bytes="+total.freeBytes()+"+; used bytes="+total.usedBytes()+"; total bytes="+total.totalBytes());
        for(IndexStatistics s:stats)
            LOG.info("  Object size "+s.itemSize()+" used="+s.usedCount()+"; free="+s.freeCount()+"; total="+s.totalCount());
    }

    public IndexStatistics[]getIndexStatistics(IndexStatistics grandTotal) {
    	IndexStatistics[]stats=getIndexStatistics();
        long totalfree=0,totalused=0;
        for(IndexStatistics stat:stats) {
            totalfree+=stat.freeBytes();
            totalused+=stat.usedBytes();
        }
        grandTotal.setTo(totalfree,totalused,1);
        return stats;
    }

    public IndexStatistics[]getIndexStatistics() {
    	IndexStatistics[]stats=new IndexStatistics[BUCKET_SIZES.length];
        for(int i=0;i<stats.length;++i)
            stats[i]=mBucketsBySize[i].statistics();
    	return stats;
    }
    
    public long freeBlock(long freeList[]) {
    	long sz=0;
        for(int i=0;i<freeList.length;++i)
            sz+=freeBlock(freeList[i]);
        return sz;
    }

}
