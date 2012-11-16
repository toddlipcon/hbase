package org.apache.hadoop.hbase.io.hfile.flash;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;

public class HeapForwardMap implements ForwardMap {
    /* cache item in cache. We expect this to be where most memory goes. Java uses 8 bytes
     * just for object headers; after this, we want to use as little as possible - so we
     * only use 8 bytes, but in order to do so end up messing around with all this Java
     * casting stuff. Offset stored as 5 bytes that make up the long. Doubt we'll see
     * devices this big for ages. Offsets are divided by 256. So 5 bytes gives us 256TB or so.
     */
    static class CompactEntry implements Serializable {
    	private int mOffsetBase;
    	private short mLengthIndex;
    	private byte mOffset1;
    	private byte mDeserialiserIndex;
        CompactEntry(FlashCache.Entry fe) {
            setOffset(fe.offset());
            mLengthIndex=fe.lengthIndex();
            mDeserialiserIndex=fe.deserialiserIndex();
        }
        
        CompactEntry(long offset,short lengthIndex,byte deserialiserIndex) {
            setOffset(offset);
            mLengthIndex=lengthIndex;
            mDeserialiserIndex=deserialiserIndex;
        }

        void toEntry(FlashCache.Entry out) {
            out.setOffset(offset());
            out.setLengthIndex(mLengthIndex);
            out.setDeserialiserIndex(mDeserialiserIndex);
        }

        private long offset() { // Java has no unsigned numbers
            long o=((long)mOffsetBase)&0xFFFFFFFF;
            o+=(((long)(mOffset1))&0xFF)<<32;
            return o<<8;
        }

        private void setOffset(long value) {
            assert (value&0xFF)==0;
            value>>=8;
            mOffsetBase=(int)value;
            mOffset1=(byte)(value>>32);
        }
        public short lengthIndex() { return mLengthIndex; }
        public void setLengthIndex(short value) { mLengthIndex=value; }
        public byte deserialiserIndex() { return mDeserialiserIndex; }
        public void setDeserialiserIndex(byte value) { mDeserialiserIndex=value; }
    }

    public class HeapReference implements ForwardMap.Reference {
    	BlockCacheKey mKey;
    	CompactEntry mValue;
    	public void setTo(ConcurrentHashMap.Entry<BlockCacheKey,CompactEntry>val) {
            mKey=val.getKey();
            mValue=val.getValue();
    	}
    	public void setTo(BlockCacheKey key) {
            mKey=key;
            mValue=mMap.get(mKey);
    	}
        public void getEntryTo(FlashCache.Entry entry) {
            entry.setOffset(mValue.offset());
            entry.setLengthIndex(mValue.lengthIndex());
            entry.setDeserialiserIndex(mValue.deserialiserIndex());
        }

        public int getHashCode() {
            return mKey.fcHashCode();
        }
        public BlockCacheKey key() { return mKey; }
    }

    public class HeapReferenceList implements ForwardMap.ReferenceList {
        private ArrayList<BlockCacheKey>mKeys=new ArrayList<BlockCacheKey>();
        private class ListIterator implements Iterator<Reference> {
            HeapReference mRef=new HeapReference();
            int mIndex=0;

            public Reference next() {
            	mRef.setTo(mKeys.get(mIndex++));
                return mRef;
            }

            public boolean hasNext() {
                return mIndex<mKeys.size();
            }

            public void remove() {
            }
        }
        public int size() {
            return mKeys.size();
        }

        public Iterator<ForwardMap.Reference>iterator() {
            return new ListIterator();
        }

        public void add(Reference ref) {
            assert ref instanceof HeapReference;
            assert ((HeapReference)ref).key()!=null;
            assert mKeys!=null;
            mKeys.add(((HeapReference)ref).key());
        }
    }
    
    private class MapIterator implements Iterator<ForwardMap.Reference> {
        HeapReference mRef;
        Iterator<ConcurrentHashMap.Entry<BlockCacheKey,CompactEntry>>mIterator;
        MapIterator() {
            mRef=new HeapReference();
            mIterator=mMap.entrySet().iterator();
        }
        public boolean hasNext() {
            return mIterator.hasNext();
        }

        public Reference next() {
            mRef.setTo(mIterator.next());
            return mRef;
        }

        public void remove() {
        }
    }

    public Iterator<ForwardMap.Reference>iterator() {
        return new MapIterator();
    }

    public ForwardMap.ReferenceList factoryReferenceList() {
        return new HeapReferenceList();
    }

    public HeapForwardMap(int buckets) {
        mMap=new ConcurrentHashMap<BlockCacheKey,CompactEntry>(buckets);
    }

    public boolean get(BlockCacheKey key,FlashCache.Entry output) {
        CompactEntry ce=mMap.get(key);
        if(ce==null)
            return false;
        ce.toEntry(output);
        return true;
    }

    public boolean remove(BlockCacheKey key) {
        CompactEntry ce=mMap.remove(key);
        return ce!=null;
    }
    
    public boolean containsKey(BlockCacheKey key) {
    	return mMap.containsKey(key);
    }

    public void remove(Reference ref) {
        mMap.remove(((HeapReference)ref).key());
    }

    private ConcurrentHashMap<BlockCacheKey,CompactEntry>mMap;

    public void put(BlockCacheKey key, FlashCache.Entry entry) {
        mMap.put(key,new CompactEntry(entry));
    }

    public void persistTo(ObjectOutputStream s) throws IOException {
    	s.writeObject(mMap);
    }
    public void retrieveFrom(ObjectInputStream s) throws IOException {
    	try {
			mMap=(ConcurrentHashMap<BlockCacheKey,CompactEntry>)s.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
    }
}
