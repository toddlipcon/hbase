package org.apache.hadoop.hbase.io.hfile;

import java.util.HashMap;
import java.util.Map;

public class CacheableDeserializerFactory {
	static private Map<Integer,CacheableDeserializer<Cacheable>> factories=new HashMap<Integer,CacheableDeserializer<Cacheable>>();
	public static void registerDeserializer(CacheableDeserializer<Cacheable>cd,int idx) {
		synchronized(factories) {
			factories.put(idx,cd);
		}
	}
	public static CacheableDeserializer<Cacheable>getDeserializer(int idx) {
		return factories.get(idx);
	}
}
