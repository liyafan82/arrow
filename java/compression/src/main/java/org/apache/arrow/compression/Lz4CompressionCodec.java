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

package org.apache.arrow.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;

import io.netty.util.internal.PlatformDependent;

/**
 * Compression codec for the LZ4 algorithm.
 */
public class Lz4CompressionCodec extends AbstractCompressionCodec {

  protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
    byte[] inBytes = new byte[(int) uncompressedBuffer.writerIndex()];
    PlatformDependent.copyMemory(uncompressedBuffer.memoryAddress(), inBytes, 0, uncompressedBuffer.writerIndex());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (InputStream in = new ByteArrayInputStream(inBytes);
         OutputStream out = new FramedLZ4CompressorOutputStream(baos)) {
      IOUtils.copy(in, out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    byte[] outBytes = baos.toByteArray();

    ArrowBuf compressedBuffer = allocator.buffer(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + outBytes.length);
    PlatformDependent.copyMemory(
        outBytes, 0, compressedBuffer.memoryAddress() + CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, outBytes.length);
    compressedBuffer.writerIndex(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + outBytes.length);
    return compressedBuffer;
  }

  protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
    long decompressedLength = readUncompressedLength(compressedBuffer);

    byte[] inBytes = new byte[(int) (compressedBuffer.writerIndex() - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH)];
    PlatformDependent.copyMemory(
        compressedBuffer.memoryAddress() + CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, inBytes, 0, inBytes.length);
    ByteArrayOutputStream out = new ByteArrayOutputStream((int) decompressedLength);
    try (InputStream in = new FramedLZ4CompressorInputStream(new ByteArrayInputStream(inBytes))) {
      IOUtils.copy(in, out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    byte[] outBytes = out.toByteArray();
    ArrowBuf decompressedBuffer = allocator.buffer(outBytes.length);
    PlatformDependent.copyMemory(outBytes, 0, decompressedBuffer.memoryAddress(), outBytes.length);
    decompressedBuffer.writerIndex(decompressedLength);
    return decompressedBuffer;
  }

  @Override
  public CompressionUtil.CodecType getCodecType() {
    return CompressionUtil.CodecType.LZ4_FRAME;
  }
}
