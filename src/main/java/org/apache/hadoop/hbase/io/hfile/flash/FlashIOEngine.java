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
public class FlashIOEngine implements IOEngine {
    static final Log LOG = LogFactory.getLog(FlashIOEngine.class);

    private String mFilePath;
    private long mFileSize;
    private FileChannel mFileChannel=null;

	public FlashIOEngine(String filePath,long fileSize) throws IOException {
        mFilePath=filePath;
        mFileSize=fileSize;
    	RandomAccessFile raf=null;
        try {
            raf=new RandomAccessFile(filePath,"rw");
            raf.setLength(fileSize);
            mFileChannel=raf.getChannel();
        } catch(java.io.FileNotFoundException fex) {
            LOG.error("Can't create Flash cache file; flash cache disabled: "+filePath,fex);
            throw fex;
        } catch(IOException ioex) {
            LOG.error("Can't extend Flash cache file; insufficient space for "+fileSize+" bytes?",ioex);
            if(raf!=null)
                raf.close();
            throw ioex;
        }
	}

    public boolean isPersistent() {
    	return true;
    }
	
	public void read(ByteBuffer bb,long offset) throws IOException {
        mFileChannel.read(bb,offset);
    }

    public void write(ByteBuffer bb,long offset) throws IOException {
        mFileChannel.write(bb,offset);
    }
    public void sync() throws IOException {
    	mFileChannel.force(true);
    }
    public void shutdown() {
    	try {
    		mFileChannel.close();
    	} catch(IOException ex) {
    		LOG.error("Can't shut down cleanly",ex);
    	}
    }
}
