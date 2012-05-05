package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Charsets;
import com.google.common.io.Files;


public class TestBufferChain {
  private File tmpFile;
  
  private static final byte[][] HELLO_WORLD_CHUNKS = new byte[][] {
      "hello".getBytes(Charsets.UTF_8),
      " ".getBytes(Charsets.UTF_8),
      "world".getBytes(Charsets.UTF_8)
  };

  @Before
  public void setup() throws IOException {
    tmpFile = File.createTempFile("TestBufferChain", "txt");
  }
  
  @After
  public void teardown() {
    tmpFile.delete();
  }

  @Test
  public void testChainChunkBiggerThanWholeArray() throws IOException {
    ByteBuffer[] bufs = wrapArrays(HELLO_WORLD_CHUNKS);
    BufferChain chain = new BufferChain(bufs);
    writeAndVerify(chain, "hello world", 8192);
    assertNoRemaining(bufs);
  }

  @Test
  public void testChainChunkBiggerThanSomeArrays() throws IOException {
    ByteBuffer[] bufs = wrapArrays(HELLO_WORLD_CHUNKS);
    BufferChain chain = new BufferChain(bufs);
    writeAndVerify(chain, "hello world", 3);
    assertNoRemaining(bufs);
  }
  
  @Test
  public void testLimitOffset() throws IOException {
    ByteBuffer[] bufs = new ByteBuffer[] {
        stringBuf("XXXhelloYYY", 3, 5),
        stringBuf(" ", 0, 1),
        stringBuf("XXXXworldY", 4, 5) };
    BufferChain chain = new BufferChain(bufs);
    writeAndVerify(chain , "hello world", 3);
    assertNoRemaining(bufs);
  }
  
  @Test
  public void testWithSpy() throws IOException {
    ByteBuffer[] bufs = new ByteBuffer[] {
        stringBuf("XXXhelloYYY", 3, 5),
        stringBuf(" ", 0, 1),
        stringBuf("XXXXworldY", 4, 5) };
    BufferChain chain = new BufferChain(bufs);
    FileOutputStream fos = new FileOutputStream(tmpFile);
    FileChannel ch = Mockito.spy(fos.getChannel());
    try {
      chain.writeChunk(ch, 2);
      assertEquals("he", Files.toString(tmpFile, Charsets.UTF_8));
      chain.writeChunk(ch, 2);
      assertEquals("hell", Files.toString(tmpFile, Charsets.UTF_8));
      chain.writeChunk(ch, 3);
      assertEquals("hello w", Files.toString(tmpFile, Charsets.UTF_8));
      chain.writeChunk(ch, 8);
      assertEquals("hello world", Files.toString(tmpFile, Charsets.UTF_8));
    } finally {
      ch.close();
    }
  }

  private ByteBuffer stringBuf(String string, int position, int length) {
    ByteBuffer buf = ByteBuffer.wrap(string.getBytes(Charsets.UTF_8));
    buf.position(position);
    buf.limit(position + length);
    assertTrue(buf.hasRemaining());
    return buf;
  }
  
  private void assertNoRemaining(ByteBuffer[] bufs) {
    for (ByteBuffer buf : bufs) {
      assertFalse(buf.hasRemaining());
    }
  }

  private ByteBuffer[] wrapArrays(byte[][] arrays) {
    ByteBuffer[] ret = new ByteBuffer[arrays.length];
    for (int i = 0; i < arrays.length; i++) {
      ret[i] = ByteBuffer.wrap(arrays[i]);
    }
    return ret;
  }

  private void writeAndVerify(BufferChain chain, String string, int chunkSize)
      throws IOException {
    FileOutputStream fos = new FileOutputStream(tmpFile);
    FileChannel ch = fos.getChannel();
    try {
      long remaining = string.length();
      while (chain.hasRemaining()) {
        long n = chain.writeChunk(ch, chunkSize);
        assertTrue(n == chunkSize || n == remaining);
        remaining -= n;
      }
      assertEquals(0, remaining);
    } finally {
      fos.close();
    }
    assertFalse(chain.hasRemaining());
    assertEquals(string, Files.toString(tmpFile, Charsets.UTF_8));
  }
}
