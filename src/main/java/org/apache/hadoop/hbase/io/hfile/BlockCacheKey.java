/**
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
package org.apache.hadoop.hbase.io.hfile;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * Cache Key for use with implementations of {@link BlockCache}
 */
public class BlockCacheKey implements HeapSize, java.io.Serializable {
  private String hfileName;
  private long offset;
  private DataBlockEncoding encoding;

  public BlockCacheKey(String file, long offset, DataBlockEncoding encoding,
      BlockType blockType) {
    this.hfileName = file;
    this.offset = offset;
    // We add encoding to the cache key only for data blocks. If the block type
    // is unknown (this should never be the case in production), we just use
    // the provided encoding, because it might be a data block.
    this.encoding = (blockType == null || blockType.isData()) ? encoding :
        DataBlockEncoding.NONE;
  }

  /**
   * Construct a new BlockCacheKey
   * @param file The name of the HFile this block belongs to.
   * @param offset Offset of the block into the file
   */
  public BlockCacheKey(String file, long offset) {
    this(file, offset, DataBlockEncoding.NONE, null);
  }

  @Override
  public int hashCode() {
    return hfileName.hashCode() * 127 + (int) (offset ^ (offset >>> 32)) +
        encoding.ordinal() * 17;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof BlockCacheKey) {
      BlockCacheKey k = (BlockCacheKey) o;
      return offset == k.offset
          && (hfileName == null ? k.hfileName == null : hfileName
              .equals(k.hfileName));
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return hfileName + "_" + offset
        + (encoding == DataBlockEncoding.NONE ? "" : "_" + encoding);
  }

  /**
   * Strings have two bytes per character due to default Java Unicode encoding
   * (hence length times 2).
   */
  @Override
  public long heapSize() {
    return ClassSize.align(ClassSize.OBJECT + 2 * hfileName.length() +
        Bytes.SIZEOF_LONG + 2 * ClassSize.REFERENCE);
  }

  // can't avoid this unfortunately
  /**
   * @return The hfileName portion of this cache key
   */
  public String getHfileName() {
    return hfileName;
  }

  public DataBlockEncoding getDataBlockEncoding() {
    return encoding;
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
	  out.writeUTF(hfileName);
	  out.writeLong(offset);
	  out.writeObject(encoding);
  }

  private void readObject(java.io.ObjectInputStream in) throws IOException,ClassNotFoundException {
	  hfileName=in.readUTF();
	  offset=in.readLong();
	  encoding=(DataBlockEncoding)in.readObject();
  }

    ////////////////////////////////////////////////////////////////////////////////
    // Added by NAC - stuff for high performance repeated getting of hashes and digests
    // This can all go away if we mod this thing to store the underlying binary data
    // behind the filename instead...
    ////////////////////////////////////////////////////////////////////////////////
    static final Log LOG = LogFactory.getLog(BlockCacheKey.class);
    static final int DIGEST_CACHE_SIZE=512;
    static private ArrayBlockingQueue<MessageDigest>MDigests=new
        ArrayBlockingQueue<MessageDigest>(DIGEST_CACHE_SIZE);
    private byte[]mBytes;
    private int mHashCode=-1;
    private void reduce() {
        java.security.MessageDigest md=null;
        byte[]buffer=new byte[hfileName.length()+10];
        int i=0;
        for(;i<hfileName.length();++i)
            buffer[i]=(byte)hfileName.charAt(i);
        buffer[i++]=(byte)offset;
        buffer[i++]=(byte)(offset>>8);
        buffer[i++]=(byte)(offset>>16);
        buffer[i++]=(byte)(offset>>24);
        buffer[i++]=(byte)(offset>>32);
        buffer[i++]=(byte)(offset>>40);
        buffer[i++]=(byte)(offset>>48);
        buffer[i++]=(byte)(offset>>56);
        buffer[i++]=(byte)encoding.getId();
        buffer[i++]=(byte)((encoding.getId()>>8));
        try {
            md=getDigest();
            mBytes=md.digest(buffer); // also resets the digest per javadoc
            mHashCode=mBytes[31];
            int mul=1;
            for(i=0;i<32;++i,mul*=31)
                mHashCode+=(mBytes[i]+30)*mul;
        } finally {
        	if(md!=null)
        		releaseDigest(md);
        }
    }
        
    public int fcHashCode() {
        if(mHashCode==-1)
            reduce();
        return mHashCode&0x7FFFFFFF;
    }

    public int fcHashCode(int modulo) {
        return fcHashCode()%modulo;
    }

    public byte[]fcKeyBytes() {
        if(mHashCode==-1)
            reduce();
        return mBytes;
    }

    public static final int fcKeyBytesLength() {
        MessageDigest dg=null;
        try {
            dg=getDigest();
            return dg.getDigestLength();
        } finally {
            releaseDigest(dg);
        }
    }

    static private MessageDigest getDigest() {
        MessageDigest dg=MDigests.poll();
        if(dg==null) {
            try {
                dg=java.security.MessageDigest.getInstance("SHA-256");
            } catch(NoSuchAlgorithmException ex) {
                assert dg!=null; // This should never fail
                LOG.fatal("Can't find necessary message digest... Should never happen");
            }
        }
        return dg;
    }

    static private void releaseDigest(MessageDigest dg) {
        MDigests.offer(dg);
    }
}
