package org.apache.hadoop.hbase.io.hfile.flash;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/* ByteBuffer isn't thread-safe and stateless. It also only support up
 * to a signed 'int' in capacity. This sucks. So this class lets us create
 * arrays of entities that are a certain number of bytes in size, lock
 * and unlock them, and get back ByteBuffers to meddle with them.
 * We use direct memory allocation here, and have a target size of buffer
 * to use (currently 4MB)
 */
public final class DirectArray {
    static final Log LOG = LogFactory.getLog(DirectArray.class);    

    static final int TARGET_BUFFER_SIZE=4*1024*1024;
    private ByteBuffer mBuffers[];
    private Lock mLocks[];
    private int mObjectSize,mObjectsPerBuffer;
    private long mObjectCount;
    private long mBufferCount,mBufferSize;
    private boolean mDirect;

    public DirectArray(int objSize,long objCount,boolean direct) {
        int targetBufferSize=TARGET_BUFFER_SIZE;
        if(targetBufferSize>(objSize*objCount/16))
            targetBufferSize=roundUp(objSize*objCount/16,32768);
        mDirect=direct;
        mObjectSize=objSize;
        mObjectCount=objCount;
        mObjectsPerBuffer=targetBufferSize/mObjectSize;
        mBufferSize=mObjectsPerBuffer*mObjectSize;
        mBufferCount=(int)(roundUp(mObjectCount,mObjectsPerBuffer)/mObjectsPerBuffer);
        LOG.info("DirectArray: Allocating "+mBufferCount+" direct buffers for "+mObjectCount+" objects of size "+mObjectSize);
        mBuffers=new ByteBuffer[(int)mBufferCount+1];
        mLocks=new Lock[(int)mBufferCount+1];
        for(int i=0;i<=mBufferCount;++i) {
            mLocks[i]=new ReentrantLock();
            if(i<mBufferCount)
                mBuffers[i]=direct?ByteBuffer.allocateDirect((int)mBufferSize):ByteBuffer.allocate((int)mBufferSize);
            else
                mBuffers[i]=ByteBuffer.allocate(0);
        }
    }

    private int roundUp(long n,long to) {
        return (int)(((n+to-1)/to)*to);
    }

    void get(long itemNo,byte[]dst) {
        assert itemNo<mObjectCount;
        assert(dst.length==mObjectSize);
        int buffer=(int)(itemNo/mObjectsPerBuffer),offset=(int)(itemNo%mObjectsPerBuffer);
        ByteBuffer bb=mBuffers[buffer];
        mLocks[buffer].lock();
        try {
            bb.limit(offset*mObjectSize+mObjectSize).position(offset*mObjectSize);
            bb.get(dst);
        } finally {
            mLocks[buffer].unlock();
        }
    }
	
    void put(long itemNo,byte[]src) {
        assert itemNo<mObjectCount;
        assert(src.length==mObjectSize);
        int buffer=(int)(itemNo/mObjectsPerBuffer),offset=(int)(itemNo%mObjectsPerBuffer);
        ByteBuffer bb=mBuffers[buffer];
        mLocks[buffer].lock();
        try {
            bb.limit(offset*mObjectSize+mObjectSize).position(offset*mObjectSize);
            bb.put(src);
        } finally {
            mLocks[buffer].unlock();
        }
    }

    /* WARNING: Dangerous method. You WILL get stuffed in weird and unpredictable ways
     * if you try to use this while someone else is using the DirectArray. All thanks to
     * the lack of stateless methods... So you must ensure up the call stack that
     * concurrency isn't possible!
     */
    ByteBuffer getBufferUnlocked(long itemNo) {
        int buffer=(int)(itemNo/mObjectsPerBuffer),offset=(int)(itemNo%mObjectsPerBuffer);
        ByteBuffer bb=mBuffers[buffer];
        bb.limit(offset*mObjectSize+mObjectSize).position(offset*mObjectSize);
        return bb;
    }

    ByteBuffer lockBuffer(long itemNo) {
        int buffer=(int)(itemNo/mObjectsPerBuffer),offset=(int)(itemNo%mObjectsPerBuffer);
        ByteBuffer bb=mBuffers[buffer];
        mLocks[buffer].lock();
        bb.limit(offset*mObjectSize+mObjectSize).position(offset*mObjectSize);
        return bb;
    }
	
    void unlockBuffer(long itemNo) {
        int buffer=(int)(itemNo/mObjectsPerBuffer);
        mLocks[buffer].unlock();
    }

    void getMultiple(long start,int len,byte[]dst) {
        getMultiple(start,len,dst,0);
    }
	
    void getMultiple(long start,int len,byte[]dst,int dstOffset) {
        multiple(start,len,dst,dstOffset, new Accessor() {
                public void access(ByteBuffer bb,byte[]array,int arrayIdx,int len) {
                    bb.get(array,arrayIdx,len);
                }
            });
    }
	
    void putMultiple(long start,int len,byte[]dst) {
        putMultiple(start,len,dst,0);
    }
	
    void putMultiple(long start,int len,byte[]src,int srcOffset) {
        multiple(start,len,src,srcOffset, new Accessor() {
                public void access(ByteBuffer bb,byte[]array,int arrayIdx,int len) {
                    bb.put(array,arrayIdx,len);
                }
            });
    }

    interface Accessor {
        void access(ByteBuffer bb,byte[] array,int arrayIdx,int len);
    }
	
    void multiple(long start,int len,byte[]array,int offset,Accessor op) {
        long end=start+len,endItem=end-1;
        int startBuffer=(int)(start/mObjectsPerBuffer),startOffset=(int)(start%mObjectsPerBuffer);
        int endBuffer=(int)(end/mObjectsPerBuffer),endOffset=(int)(end%mObjectsPerBuffer);
        assert array.length>=len+offset;
        for(int i=startBuffer;i<=endBuffer;++i)
            mLocks[i].lock();
        try {
            int srcIndex=0,cnt=-1;
            for(int i=startBuffer;i<=endBuffer;++i) {
                ByteBuffer bb=mBuffers[i];
                if(i==startBuffer) {
                    cnt=(mObjectsPerBuffer-startOffset)*mObjectSize;
                    if(cnt>len)
                        cnt=len;
                    bb.limit(startOffset*mObjectSize+cnt).position(startOffset*mObjectSize);
                } else if (i==endBuffer) {
                    cnt=endOffset*mObjectSize;
                    bb.limit(cnt).position(0);
                } else {
                    cnt=mObjectsPerBuffer*mObjectSize;
                    bb.limit(cnt).position(0);
                }
                op.access(bb,array,srcIndex+offset,cnt);
                srcIndex+=cnt;
            }
            assert srcIndex==len*mObjectSize;
        } finally {
            for(int i=endBuffer;i>=startBuffer;--i)
                mLocks[i].unlock();
        }
    }

    void incrementalWrite(IOEngine io,byte[]dirty) throws java.io.IOException {
        assert dirty.length==mObjectCount;
        int curBuffer=0,curOffset=0;
        mLocks[curBuffer].lock();
    	ByteBuffer bb=mBuffers[curBuffer];
        try {
	        for(int i=0;i<mObjectCount;++i) {
	        	++curOffset;
	        	if(dirty[i]!=0) {
	        		bb.limit(curOffset*mObjectSize+mObjectSize).position(curOffset*mObjectSize);
	        		io.write(bb,(long)i*(long)mObjectSize);
	        	}
	        	if(curOffset==mObjectsPerBuffer) {
	        		curOffset=0;
	        		mLocks[curBuffer].unlock();
	        		mLocks[++curBuffer].lock();
	            	bb=mBuffers[curBuffer];
	        	}
	        }
        } finally {
        	mLocks[curBuffer].unlock();
        }
    }

    public void persistTo(ObjectOutputStream oos) throws IOException {
    	oos.writeInt(mObjectSize);
    	oos.writeLong(mObjectCount);
    	oos.writeBoolean(mDirect);
    	for(int i=0;i<mBufferCount;++i) {
			mLocks[i].lock();
    		try {
    			ByteBuffer bb=mBuffers[i];
    			bb.limit(mObjectSize*mObjectsPerBuffer).position(0);
    			Channels.newChannel(oos).write(bb);
    		} finally {
    			mLocks[i].unlock();
    		}
    	}
    }
    
    private void readFrom(ObjectInputStream iis) throws IOException {
    	ReadableByteChannel chan=Channels.newChannel(iis);
    	for(int i=0;i<mBufferCount;++i) {
			mLocks[i].lock();
    		try {
    			ByteBuffer bb=mBuffers[i];
    			bb.limit(mObjectSize*mObjectsPerBuffer).position(0);
    			chan.read(bb);
    		} finally {
    			mLocks[i].unlock();
    		}
    	}
    }
    
    static public DirectArray fromInputStream(ObjectInputStream ois) throws IOException {
    	int objectSize=ois.readInt();
    	long objectCount=ois.readLong();
    	boolean direct=ois.readBoolean();
    	DirectArray da=new DirectArray(objectSize,objectCount,direct);
    	da.readFrom(ois);
    	return da;
    }
}
