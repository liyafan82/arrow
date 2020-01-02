/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector.ipc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.apache.arrow.vector.ipc.message.FBSerializable;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.flatbuffers.FlatBufferBuilder;

import io.netty.buffer.ArrowBuf;

/**
 * Wrapper around a WritableByteChannel that maintains the position as well adding
 * some common serialization utilities.
 *
 * <p>All write methods in this class follow full write semantics, i.e., write calls
 * only return after requested data has been fully written. Note this is different
 * from java WritableByteChannel interface where partial write is allowed
 */
public class WriteChannel implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(WriteChannel.class);

  // the maximum padding has 7 bytes
  private static byte[] zeroBytes = new byte[7];

  private long currentPosition = 0;

  private final WritableByteChannel out;

  public WriteChannel(WritableByteChannel out) {
    this.out = out;
  }

  @Override
  public void close() throws IOException {
    out.close();
  }

  public long getCurrentPosition() {
    return currentPosition;
  }

  public long write(byte[] buffer) throws IOException {
    return write(ByteBuffer.wrap(buffer));
  }

  long write(byte[] buffer, int offset, int length) throws IOException {
    return write(ByteBuffer.wrap(buffer, offset, length));
  }

  /**
   * Writes <zeroCount>zeroCount</zeroCount> zeros the underlying channel.
   */
  public long writeZeros(long zeroCount) throws IOException {
    long bytesWritten = 0;
    while (zeroCount > 0) {
      int bytesToWrite = (int)Math.min(zeroCount, Integer.MAX_VALUE);
      bytesWritten += write(new byte[bytesToWrite]);
      zeroCount -= Integer.MAX_VALUE;
    }
    return bytesWritten;
  }

  /**
   * Writes enough bytes to align the channel to an 8-byte boundary.
   */
  public long align() throws IOException {
    int trailingByteSize = (int) (currentPosition % 8);
    if (trailingByteSize != 0) { // align on 8 byte boundaries
      return write(zeroBytes, 0, 8 - trailingByteSize);
    }
    return 0;
  }

  /**
   * Writes all data from <code>buffer</code> to the underlying channel.
   */
  public long write(ByteBuffer buffer) throws IOException {
    long length = buffer.remaining();
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Writing buffer with size: {}", length);
    }
    while (buffer.hasRemaining()) {
      out.write(buffer);
    }
    currentPosition += length;
    return length;
  }

  /**
   * Writes <code>v</code> in little-endian format to the underlying channel.
   */
  public long writeIntLittleEndian(int v) throws IOException {
    byte[] outBuffer = new byte[4];
    MessageSerializer.intToBytes(v, outBuffer);
    return write(outBuffer);
  }

  /**
   * Writes the buffer to the underlying channel.
   */
  public void write(ArrowBuf buffer) throws IOException {
    long bytesWritten = 0;
    while (bytesWritten < buffer.readableBytes()) {
      int bytesToWrite = (int)Math.min(Integer.MAX_VALUE, buffer.readableBytes() - bytesWritten);
      ByteBuffer nioBuffer = buffer.nioBuffer(buffer.readerIndex() + bytesWritten,
           bytesToWrite);
      write(nioBuffer);
      bytesWritten += bytesToWrite;
    }

  }

  /**
   * Writes the serialized flatbuffer to the underlying channel.  If withSizePrefix
   * is true then the length in bytes of the buffer will first be written in little endian format.
   */
  public long write(FBSerializable writer, boolean withSizePrefix) throws IOException {
    ByteBuffer buffer = serialize(writer);
    if (withSizePrefix) {
      writeIntLittleEndian(buffer.remaining());
    }
    return write(buffer);
  }

  /**
   * Serializes writer to a ByteBuffer.
   */
  public static ByteBuffer serialize(FBSerializable writer) {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    int root = writer.writeTo(builder);
    builder.finish(root);
    return builder.dataBuffer();
  }
}
