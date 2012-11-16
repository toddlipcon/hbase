package org.apache.hadoop.hbase.io.hfile.flash;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

// Map from a type to int and vice-versa. Used for reducing bitfield item
// counts.
public final class UniqueIndexMap<T> implements Serializable {
    ConcurrentHashMap<T,Integer>mForwardMap=new ConcurrentHashMap<T,Integer>();
    ConcurrentHashMap<Integer,T>mReverseMap=new ConcurrentHashMap<Integer,T>();
    AtomicInteger mIndex=new AtomicInteger(0);

    // Map a length to an index. If we can't, allocate a new mapping. We might race here and
    // get two entries with the same deserialiser. This is fine.
    int map(T parameter) {
    	Integer ret=mForwardMap.get(parameter);
    	if(ret!=null)
            return ret.intValue();
    	int nexti=mIndex.incrementAndGet();
    	assert(nexti<Short.MAX_VALUE);
    	mForwardMap.put(parameter,nexti);
    	mReverseMap.putIfAbsent(nexti,parameter);
    	return nexti;
    }
    
    T unmap(int leni) {
    	Integer len=new Integer(leni);
    	assert mReverseMap.containsKey(len);
    	return mReverseMap.get(len);
    }
}
