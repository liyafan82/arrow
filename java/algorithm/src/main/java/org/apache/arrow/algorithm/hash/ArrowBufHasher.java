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

package org.apache.arrow.algorithm.hash;

import java.nio.ByteOrder;

import org.apache.arrow.util.Preconditions;

import io.netty.buffer.ArrowBuf;

/**
 * Utility for calculating the hash code for a consecutive memory region.
 */
public abstract class ArrowBufHasher {

  public static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

  /**
   * Calculates the hash code for a memory region.
   * @param buf the buffer for the memory region.
   * @param offset offset within the buffer for the memory region.
   * @param length length of the memory region.
   * @return the hash code.
   */
  public int hashCode(ArrowBuf buf, int offset, int length) {
    Preconditions.checkArgument(offset >= 0 && length >= 0 && offset + length <= buf.capacity());

    int hashValue = 0;
    int index = 0;
    while (index + 8 <= length) {
      long longValue = buf.getLong(index + offset);
      if (!LITTLE_ENDIAN) {
        // assume the buffer is in little endian
        longValue = Long.reverseBytes(longValue);
      }
      int longHash = getLongHashCode(longValue);
      hashValue = combineHashCode(hashValue, longHash);
      index += 8;
    }

    while (index + 4 <= length) {
      int intValue = buf.getInt(index + offset);
      if (!LITTLE_ENDIAN) {
        intValue = Integer.reverseBytes(intValue);
      }
      int intHash = getIntHashCode(intValue);
      hashValue = combineHashCode(hashValue, intHash);
      index += 4;
    }

    while (index < length) {
      byte byteValue = buf.getByte(index + offset);
      int byteHash = getByteHashCode(byteValue);
      hashValue = combineHashCode(hashValue, byteHash);
      index += 1;
    }

    return finalizeHashCode(hashValue);
  }

  /**
   * Calculates the hash code by combining the existing hash code and a new hash code.
   * @param currentHashCode the existing hash code.
   * @param newHashCode the new hash code.
   * @return the combined hash code.
   */
  public abstract int combineHashCode(int currentHashCode, int newHashCode);

  /**
   * Gets the hash code for a byte value.
   * @param byteValue the byte value.
   * @return the hash code.
   */
  public abstract int getByteHashCode(byte byteValue);

  /**
   * Gets the hash code for a integer value.
   * @param intValue the integer value.
   * @return the hash code.
   */
  public abstract int getIntHashCode(int intValue);

  /**
   * Gets the hash code for a long value.
   * @param longValue the long value.
   * @return the hash code.
   */
  public abstract int getLongHashCode(long longValue);

  /**
   * Finalize the hash code.
   * @param hashCode the current hash code.
   * @return the finalized hash code.
   */
  public abstract int finalizeHashCode(int hashCode);
}
