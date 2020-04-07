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

package org.apache.arrow.memory;

import io.netty.util.internal.PlatformDependent;

/**
 * Allocation manager based on unsafe API.
 */
public class UnsafeAllocationManager extends AllocationManager {

  public static final Factory FACTORY = new Factory();

  private final long allocatedSize;

  private final long allocatedAddress;

  protected UnsafeAllocationManager(BaseAllocator accountingAllocator, long requestedSize) {
    super(accountingAllocator);
    allocatedAddress = PlatformDependent.allocateMemory(requestedSize);
    allocatedSize = requestedSize;
  }

  @Override
  public long getSize() {
    return allocatedSize;
  }

  @Override
  protected long memoryAddress() {
    return allocatedAddress;
  }

  @Override
  protected void release0() {
    PlatformDependent.freeMemory(allocatedAddress);
  }

  /**
   * Factory for creating {@link UnsafeAllocationManager}.
   */
  public static class Factory implements AllocationManager.Factory {
    private Factory() {}

    @Override
    public AllocationManager create(BaseAllocator accountingAllocator, long size) {
      return new UnsafeAllocationManager(accountingAllocator, size);
    }
  }
}