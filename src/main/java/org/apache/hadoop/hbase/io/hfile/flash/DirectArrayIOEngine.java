package org.apache.hadoop.hbase.io.hfile.flash;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;

/* IO engine that stores data to a file on the file system
 */
public class DirectArrayIOEngine implements IOEngine {
    static final Log LOG = LogFactory.getLog(FlashIOEngine.class);
    private DirectArray mBuffer;

	public DirectArrayIOEngine(String filePath,long fileSize, boolean direct) throws IOException {
		mBuffer=new DirectArray(1, fileSize, direct);
	}
    public boolean isPersistent() {
    	return false;
    }
    public void read(ByteBuffer bb,long offset) throws IOException {
    	assert bb.hasArray();
    	mBuffer.getMultiple(offset,bb.remaining(),bb.array(),bb.arrayOffset());
    }

    public void write(ByteBuffer bb,long offset) throws IOException {
    	assert bb.hasArray();
    	mBuffer.putMultiple(offset,bb.remaining(),bb.array(),bb.arrayOffset());
    }
    public void sync() {
    }
    public void shutdown() {
    }
}
