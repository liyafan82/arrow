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

package org.apache.arrow.memory.util;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import org.apache.arrow.memory.util.hash.DirectHasher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

/**
 * Test cases for {@link ArrowBufPointer}.
 */
public class TestArrowBufPointer {

  private final int BUFFER_LENGTH = 1024;

  private BufferAllocator allocator;

  @Before
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
  }

  @After
  public void shutdown() {
    allocator.close();
  }

  @Test
  public void testArrowBufPointersEqual() {
    try (ArrowBuf buf1 = allocator.buffer(BUFFER_LENGTH);
    ArrowBuf buf2 = allocator.buffer(BUFFER_LENGTH)) {
      for (int i = 0; i < BUFFER_LENGTH / 4; i++) {
        buf1.setInt(i * 4, i * 1234);
        buf2.setInt(i * 4, i * 1234);
      }

      ArrowBufPointer ptr1 = new ArrowBufPointer(null, 0, 100);
      ArrowBufPointer ptr2 = new ArrowBufPointer(null, 100, 5032);
      assertTrue(ptr1.equals(ptr2));
      for (int i = 0; i < BUFFER_LENGTH / 4; i++) {
        ptr1.set(buf1, i * 4, 4);
        ptr2.set(buf2, i * 4, 4);
        assertTrue(ptr1.equals(ptr2));
      }
    }
  }

  @Test
  public void testArrowBufPointersHashCode() {
    final int vectorLength = 100;
    try (ArrowBuf buf1 = allocator.buffer(vectorLength * 4);
         ArrowBuf buf2 = allocator.buffer(vectorLength * 4)) {
      for (int i = 0; i < vectorLength; i++) {
        buf1.setInt(i * 4, i);
        buf2.setInt(i * 4, i);
      }

      ArrowBufPointer pointer1 = new ArrowBufPointer();
      assertEquals(0, pointer1.hashCode());

      ArrowBufPointer pointer2 = new ArrowBufPointer();
      assertEquals(0, pointer2.hashCode());

      for (int i = 0; i < vectorLength; i++) {
        pointer1.set(buf1, i * 4, 4);
        pointer2.set(buf2, i * 4, 4);

        assertEquals(pointer1.hashCode(), pointer2.hashCode());
      }
    }
  }
}
