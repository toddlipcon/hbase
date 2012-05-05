package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

class BufferChain {
  private static final int NIO_BUFFER_LIMIT = 8192;
  private ByteBuffer[] buffers;
  int remaining;
  int bufferOffset = 0;

  BufferChain(ByteBuffer ... buffers) {
    this.buffers = buffers;
    
    remaining = 0;
    for (ByteBuffer b : buffers) {
      remaining += b.remaining();
    }
  }
  
  public boolean hasRemaining() {
    return remaining > 0;
  }
  
  public long writeChunk(GatheringByteChannel channel, int chunkSize) throws IOException {
    int chunkRemaining = chunkSize;
    ByteBuffer lastBuffer = null;
    int bufCount = 0;
    int restoreLimit = -1;

    while (chunkRemaining > 0 && bufferOffset + bufCount < buffers.length) {
      lastBuffer = buffers[bufferOffset + bufCount];
      if (!lastBuffer.hasRemaining()) {
        bufferOffset++;
        continue;
      }
      bufCount++;
      
      if (lastBuffer.remaining() > chunkRemaining) {
        restoreLimit = lastBuffer.limit();
        lastBuffer.limit(lastBuffer.position() + chunkRemaining);
        chunkRemaining = 0;
        break;
      } else {
        chunkRemaining -= lastBuffer.remaining();
      }
    }
    assert lastBuffer != null;
    if (chunkRemaining == chunkSize) {
      assert !hasRemaining();
      // no data left to write
      return 0;
    }
    
    try {
      long ret = channel.write(buffers, bufferOffset, bufCount);
      if (ret > 0) {
        remaining -= ret;
      }
      return ret;
    } finally {
      if (restoreLimit >= 0) {
        lastBuffer.limit(restoreLimit);
      }
    }
  }
  
  
  
}
