package org.apache.hadoop.hbase.io.hfile.flash;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.flash.FlashCache.Entry;

/* Big hash table that stores keys and values inline to avoid allocating tons of
 * objects on the heap. Definitely not as easy to program to as the built-in
 * stuff, but more runtime-efficient.
 * We use multiple underlying byte buffers since the ByteBuffer itself doens't
 * have any stateless methods that work with multiple threads concurrently.
 */
public final class DirectHashForwardMap implements ForwardMap {
    private DirectArray mStore;
    private byte[]mUsed;
    private int mBufferCount,mKeyLength,mValueLength,mSlotCount;

    public interface GetHandlerHashed {
        int dhHashCode();
        byte[] keyArray();
        void bytesToValue(ByteBuffer input,int len);
    }

    public interface RemoveHandlerHashed {
        int dhHashCode();
        byte[] keyArray();
    }

    public interface PutHandlerHashed {
        int dhHashCode();
        void keyToBytes(ByteBuffer output);
        void valueToBytes(ByteBuffer output);
    }

    public DirectHashForwardMap(int items,boolean direct) {
    	configure((int)recommendedHashSize(items),BlockCacheKey.fcKeyBytesLength(),8+2+1,direct);
    }

    static public long recommendedHashSize(long dataPoints) {
        return dataPoints*2+1;
    }

    public void configure(int slotCount,int keyBytes,int valueBytes,boolean direct) {
        assert (slotCount&1)==1; // At least be odd, if not prime
        assert keyBytes>0&&valueBytes>0;
        int objSize=keyBytes+valueBytes+4; // for hash code
        mStore=new DirectArray(objSize,slotCount,direct);
        mUsed=new byte[slotCount];
        for(int i=0;i<slotCount;++i) {
            mUsed[i]=0;
        }
        mKeyLength=keyBytes;
        mSlotCount=slotCount;
        mValueLength=valueBytes;
    }

    public boolean get(BlockCacheKey key,FlashCache.Entry out) {
        int keyHash=key.fcHashCode(),slot=keyHash%mSlotCount;
        if(mUsed[slot]==0)
            return false;
        ByteBuffer bb=mStore.lockBuffer(slot);
        try {
            // Compare contents of store hash and key to keyBytes. Match - return value; no match - miss
            if(bb.getInt()!=keyHash)
                return false;
            byte[]realKey=key.fcKeyBytes();
            assert mKeyLength==realKey.length;
            for(int i=0;i<mKeyLength;++i)
                if(bb.get()!=realKey[i])
                    return false;
            out.setOffset(bb.getLong());
            out.setLengthIndex(bb.getShort());
            out.setDeserialiserIndex(bb.get());
            return true;
        } finally {
            mStore.unlockBuffer(slot);
        }
    }

    /* If this turns out to be a performance bottleneck, we could stop at the hash code as long as
     * the caller is OK with false positives...
     */
    public boolean containsKey(BlockCacheKey key) {
        int keyHash=key.fcHashCode(),slot=keyHash%mSlotCount;
        if(mUsed[slot]==0)
            return false;
        ByteBuffer bb=mStore.lockBuffer(slot);
        try {
            // Compare contents of store hash and key to keyBytes. Match - return value; no match - miss
            if(bb.getInt()!=keyHash)
                return false;
            byte[]realKey=key.fcKeyBytes();
            assert mKeyLength==realKey.length;
            for(int i=0;i<mKeyLength;++i)
                if(bb.get()!=realKey[i])
                    return false;
            return true;
        } finally {
            mStore.unlockBuffer(slot);
        }
    }

    public void put(BlockCacheKey key,FlashCache.Entry entry) {
        int keyHash=key.fcHashCode(),slot=keyHash%mSlotCount;
        ByteBuffer bb=mStore.lockBuffer(slot);
        try { // Trash whatever key and value are stored there.
        	bb.putInt(key.fcHashCode());
        	bb.put(key.fcKeyBytes());
            bb.putLong(entry.offset());
            bb.putShort(entry.lengthIndex());
            bb.put(entry.deserialiserIndex());
            mUsed[slot]=1;
        } finally {
            mStore.unlockBuffer(slot);
        }
    }
	
    public boolean remove(BlockCacheKey key) {
        int keyHash=key.fcHashCode(),slot=keyHash%mSlotCount;
        if(mUsed[slot]==0)
            return false;
        ByteBuffer bb=mStore.lockBuffer(slot);
        try {
            // Compare contents of store hash and key to keyBytes. Match - return value; no match - miss
            if(bb.getInt()!=keyHash)
                return false;
            byte[]realKey=key.fcKeyBytes();
            assert mKeyLength==realKey.length;
            for(int i=0;i<mKeyLength;++i)
                if(bb.get()!=realKey[i])
                    return false;
            mUsed[slot]=0;
            return true;
        } finally {
            mStore.unlockBuffer(slot);
        }
    }
    
    public class SlotReferenceList implements ForwardMap.ReferenceList {
        private int mSlots[]=new int[30000];
        private int mSlotCount=0;

        private class SlotReferenceListIterator implements Iterator<ForwardMap.Reference> {
            SlotReference mRef=new SlotReference();
            int mIndex=0;
            public Reference next() {
                mRef.setSlot(mSlots[mIndex++]);
                return mRef;
            }
            public boolean hasNext() {
                return mIndex<mSlotCount;
            }
            public void remove() {
                mRef.remove();
            }
        }
		
        public int size() {
            return mSlotCount;
        }
        public void add(Reference ref) {
            SlotReference slref=(SlotReference)ref;
            if(mSlotCount==mSlots.length)
                mSlots=Arrays.copyOf(mSlots,mSlotCount*2);
            mSlots[mSlotCount++]=slref.slot();
        }
        public Iterator<Reference> iterator() {
            return new SlotReferenceListIterator();
        }
    }
    
    public ReferenceList factoryReferenceList() {
        return new SlotReferenceList();
    }

    /* The iterator does not lock/unlock the underlying data structure. So if you do stuff
     * concurrent with it, good luck!
     */
    public class SlotIterator implements Iterator<ForwardMap.Reference> {
    	int mSlotNo;
    	private SlotReference mSlot;

    	public SlotIterator() {
            mSlot=new SlotReference();
            mSlotNo=-1;
        	while(mSlotNo<mSlotCount) {
        		++mSlotNo;
        		if(mSlotNo==mSlotCount||mUsed[mSlotNo]==1)
        			break;
        	}
    	}
        public boolean hasNext() {
            return mSlotNo<mSlotCount;
        }
        public SlotReference next() {
        	mSlot.setSlot(mSlotNo);
        	while(mSlotNo<mSlotCount) {
        		++mSlotNo;
        		if(mSlotNo==mSlotCount||mUsed[mSlotNo]==1)
        			break;
        	}
        	assert mSlot.used();
            return mSlot;
        }
        public void remove() {
            mSlot.remove();
        }
    }
    
    public class SlotReference implements ForwardMap.Reference {
    	private int mSlot;
    	private int mHashCode;
    	private byte[]mKey;
    	private long mOffset;
    	private short mLengthIndex;
    	private byte mDeserialiserIndex;
    	private boolean mGotStuff;
    	public int slot() { return mSlot; }
    	public SlotReference() {
            mSlot=0;
            mKey=new byte[mKeyLength];
            mGotStuff=false;
    	}
    	public SlotReference(int slot) {
            setSlot(slot);
    	}
    	public boolean used() {
            return mUsed[mSlot]==1;
    	}
    	public void setSlot(int slot) {
            assert slot>=0&&slot<mSlotCount;
            mSlot=slot;
            mGotStuff=false;
    	}
    	private void getStuff() {
    		assert used();
            ByteBuffer bb=mStore.getBufferUnlocked(mSlot);
            mHashCode=bb.getInt();
            bb.get(mKey);
            mOffset=bb.getLong();
            mLengthIndex=bb.getShort();
            mDeserialiserIndex=bb.get();
            mGotStuff=true;
    	}
    	public int getHashCode() {
            if(!mGotStuff)
                getStuff();
            return mHashCode;
    	}
    	public void remove() {
            mUsed[mSlot]=0;
    	}
        public void getEntryTo(Entry entry) {
            if(!mGotStuff)
                getStuff();
            entry.setOffset(mOffset);
            entry.setLengthIndex(mLengthIndex);
            entry.setDeserialiserIndex(mDeserialiserIndex);
        }
    }
    
    public void remove(Reference ref) {
        SlotReference slref=(SlotReference)ref;
        slref.remove();
    }

    public Iterator<ForwardMap.Reference> iterator() {
        return new SlotIterator();
    }

	public void persistTo(ObjectOutputStream oos) throws IOException {
		oos.writeInt(mBufferCount);
		oos.writeInt(mKeyLength);
		oos.writeInt(mValueLength);
		oos.writeInt(mSlotCount);
		oos.writeObject(mUsed);
		mStore.persistTo(oos);
	}

	public void retrieveFrom(ObjectInputStream ois) throws IOException {
		int bufferCount=ois.readInt();
		int keyLength=ois.readInt();
		int valueLength=ois.readInt();
		int slotCount=ois.readInt();
		byte[]used=null;
		try {
			used=(byte[])ois.readObject();
		} catch(ClassNotFoundException cnfe) {
			throw new IOException("Error retrieving from input stream",cnfe);
		}
		if(bufferCount!=mBufferCount)
			throw new IOException("Direct hash: Mismatched buffer count");
		if(keyLength!=mKeyLength)
			throw new IOException("Direct hash: Mismatched key length");
		if(valueLength!=mValueLength)
			throw new IOException("Direct hash: Mismatched value length");
		if(slotCount!=mSlotCount)
			throw new IOException("Direct hash: Mismatched slot count");
		mStore=DirectArray.fromInputStream(ois);
		mUsed=used;
	}
}
