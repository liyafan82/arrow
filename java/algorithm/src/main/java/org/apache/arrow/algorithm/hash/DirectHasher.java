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

/**
 * Calculate hash code by directly returning the integers.
 * This is the default and the fastest way to get the hash code.
 */
public class DirectHasher extends ArrowBufHasher {

  private final int seed;

  public DirectHasher(int seed) {
    this.seed = seed;
  }

  @Override
  public int combineHashCode(int currentHashCode, int newHashCode) {
    return currentHashCode * 37 + newHashCode;
  }

  @Override
  public int getByteHashCode(byte byteValue) {
    return Byte.hashCode(byteValue);
  }

  @Override
  public int getIntHashCode(int intValue) {
    return Integer.hashCode(intValue);
  }

  @Override
  public int getLongHashCode(long longValue) {
    return Long.hashCode(longValue);
  }

  @Override
  public int finalizeHashCode(int hashCode) {
    // finalize by the Murmur hashing algorithm
    // details can be found in
    // https://en.wikipedia.org/wiki/MurmurHash

    int c1 = 0xcc9e2d51;
    int c2 = 0x1b873593;
    int r1 = 15;
    int r2 = 13;
    int m = 5;
    int n = 0xe6546b64;

    int k = hashCode;
    k = k * c1;
    k = k << r1;
    k = k * c2;

    int hash = seed;
    hash = hash ^ k;
    hash = hash << r2;
    hash = hash * m + n;

    return hash;
  }
}
