package org.apache.hadoop.hbase.io.hfile.flash;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;

public interface IOEngine {
	boolean isPersistent();
    void read(ByteBuffer src,long offset) throws IOException;
    void write(ByteBuffer dst,long offset) throws IOException;
    void sync() throws IOException;
    void shutdown();
}
