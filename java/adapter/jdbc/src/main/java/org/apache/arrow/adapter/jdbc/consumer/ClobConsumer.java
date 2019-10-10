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

package org.apache.arrow.adapter.jdbc.consumer;

import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.VarCharVector;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

/**
 * Wrapper for consumers which consume clob type values from {@link ResultSet}.
 * Write the data to {@link org.apache.arrow.vector.VarCharVector}.
 */
public class ClobConsumer {

  /**
   * Creates a consumer for {@link VarCharVector}.
   */
  public static JdbcConsumer<VarCharVector> createConsumer(VarCharVector vector, int index, boolean nullable) {
    if (nullable) {
      return new NullableClobConsumer(vector, index);
    } else {
      return new NonNullableClobConsumer(vector, index);
    }
  }

  /**
   * Nullable consumer for clob data.
   */
  static class NullableClobConsumer extends BaseJdbcConsumer<VarCharVector> {

    protected static final int BUFFER_SIZE = 256;

    /**
     * Instantiate a ClobConsumer.
     */
    public NullableClobConsumer(VarCharVector vector, int index) {
      super(vector, index);
      if (vector != null) {
        vector.allocateNewSafe();
      }
    }

    @Override
    public void consume(ResultSet resultSet) throws SQLException {
      Clob clob = resultSet.getClob(columnIndexInResultSet);
      if (!resultSet.wasNull()) {
        if (clob != null) {
          long length = clob.length();

          int read = 1;
          int readSize = length < BUFFER_SIZE ? (int) length : BUFFER_SIZE;
          int totalBytes = 0;

          ArrowBuf dataBuffer = vector.getDataBuffer();
          ArrowBuf offsetBuffer = vector.getOffsetBuffer();
          int startIndex = offsetBuffer.getInt(currentIndex * 4);
          while (read <= length) {
            String str = clob.getSubString(read, readSize);
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);

            while ((dataBuffer.writerIndex() + bytes.length) > dataBuffer.capacity()) {
              vector.reallocDataBuffer();
            }
            PlatformDependent.copyMemory(bytes, 0,
                    dataBuffer.memoryAddress() + startIndex + totalBytes, bytes.length);

            totalBytes += bytes.length;
            read += readSize;
          }
          offsetBuffer.setInt((currentIndex + 1) * 4, startIndex + totalBytes);
          BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), currentIndex);
          vector.setLastSet(currentIndex);
        }
      }
      currentIndex++;
    }

    @Override
    public void resetValueVector(VarCharVector vector) {
      this.vector = vector;
      this.vector.allocateNewSafe();
      this.currentIndex = 0;
    }
  }

  /**
   * Non-nullable consumer for clob data.
   */
  static class NonNullableClobConsumer extends NullableClobConsumer {

    /**
     * Instantiate a ClobConsumer.
     */
    public NonNullableClobConsumer(VarCharVector vector, int index) {
      super(vector, index);
    }

    @Override
    public void consume(ResultSet resultSet) throws SQLException {
      Clob clob = resultSet.getClob(columnIndexInResultSet);
      if (clob != null) {
        long length = clob.length();

        int read = 1;
        int readSize = length < BUFFER_SIZE ? (int) length : BUFFER_SIZE;
        int totalBytes = 0;

        ArrowBuf dataBuffer = vector.getDataBuffer();
        ArrowBuf offsetBuffer = vector.getOffsetBuffer();
        int startIndex = offsetBuffer.getInt(currentIndex * 4);
        while (read <= length) {
          String str = clob.getSubString(read, readSize);
          byte[] bytes = str.getBytes(StandardCharsets.UTF_8);

          while ((dataBuffer.writerIndex() + bytes.length) > dataBuffer.capacity()) {
            vector.reallocDataBuffer();
          }
          PlatformDependent.copyMemory(bytes, 0,
                  dataBuffer.memoryAddress() + startIndex + totalBytes, bytes.length);

          totalBytes += bytes.length;
          read += readSize;
        }
        offsetBuffer.setInt((currentIndex + 1) * 4, startIndex + totalBytes);
        BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), currentIndex);
        vector.setLastSet(currentIndex);
      }
      currentIndex++;
    }
  }
}


