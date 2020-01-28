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

/**
 * A class for choosing the default allocation manager.
 */
public class DefaultAllocationManagerOption {

  /**
   * The environmental variable to set the default allocation manager type.
   */
  public static final String ALLOCATION_MANAGER_TYPE_ENV_NAME = "ARROW_ALLOCATION_MANAGER_TYPE";

  /**
   * The system property to set the default allocation manager type.
   */
  public static final String ALLOCATION_MANAGER_TYPE_PROPERTY_NAME = "arrow.allocation.manager.type";

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DefaultAllocationManagerOption.class);

  /**
   * The default allocation manager factory.
   */
  public static final AllocationManager.Factory DEFAULT_ALLOCATION_MANAGER_FACTORY =
      getDefaultAllocationManagerFactory();

  /**
   * The allocation manager type.
   */
  public enum AllocationManagerType {
    /**
     * Netty based allocation manager.
     */
    Netty,

    /**
     * Unsafe based allocation manager.
     */
    Unsafe,

    /**
     * Unknown type.
     */
    Unknown,
  }

  private static AllocationManagerType getDefaultAllocationManagerType() {
    AllocationManagerType ret = AllocationManagerType.Unknown;

    String envValue = System.getenv(ALLOCATION_MANAGER_TYPE_ENV_NAME);
    if ("netty".equals(envValue)) {
      ret = AllocationManagerType.Netty;
    } else if ("unsafe".equals(envValue)) {
      ret = AllocationManagerType.Unsafe;
    }

    // system property takes precedence
    String propValue = System.getProperty(ALLOCATION_MANAGER_TYPE_PROPERTY_NAME);
    if ("netty".equals(propValue)) {
      ret = AllocationManagerType.Netty;
    } else if ("unsafe".equals(propValue)) {
      ret = AllocationManagerType.Unsafe;
    }
    return ret;
  }

  private static AllocationManager.Factory getDefaultAllocationManagerFactory() {
    AllocationManagerType type = getDefaultAllocationManagerType();

    switch (type) {
      case Netty:
        return NettyAllocationManager.FACTORY;
      case Unsafe:
        return UnsafeAllocationManager.FACTORY;
      case Unknown:
        logger.warn("allocation manager type not specified, using netty as the default type");
        return NettyAllocationManager.FACTORY;
      default:
        throw new IllegalStateException("Unknown allocation manager type: " + type);
    }
  }
}
