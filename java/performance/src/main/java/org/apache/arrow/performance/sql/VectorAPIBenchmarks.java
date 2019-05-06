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

package org.apache.arrow.performance.sql;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.util.VectorFactory;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
public class VectorAPIBenchmarks {

  private static final int VECTOR_LENGTH = 32 * 1024;

  private BufferAllocator allocator;

  private Float8Vector safeVector;

  private Float8Vector unsafeVector;

  private Random random = new Random();

  private double safeSum;

  private double unSafeSum;

  @Setup
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024 * 16);
    safeVector = VectorFactory.createFloat8Vector(VectorFactory.VectorType.SAFE, "safe", allocator);
    safeVector.allocateNew(VECTOR_LENGTH);
    unsafeVector = VectorFactory.createFloat8Vector(VectorFactory.VectorType.UNSAFE, "unsafe", allocator);
    unsafeVector.allocateNew(VECTOR_LENGTH);

    System.setProperty("arrow.enable_unsafe_memory_access", "true");
    System.setProperty("drill.enable_unsafe_memory_access", "true");
  }

  @TearDown
  public void tearDown() {
    safeVector.clear();
    unsafeVector.clear();
    allocator.close();

    System.out.println("safe sum = " + safeSum + ", unsafe sum = " + unSafeSum);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void testSafe() {
    safeSum = 0;
    for (int i = 0; i < VECTOR_LENGTH; i++) {
      safeVector.set(i, i + 10.0);
      safeSum += safeVector.get(i);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void testUnSafe() {
    unSafeSum = 0;
    for (int i = 0; i < VECTOR_LENGTH; i++) {
      unsafeVector.set(i, i + 10.0);
      unSafeSum += unsafeVector.get(i);
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(VectorAPIBenchmarks.class.getSimpleName())
            .forks(1)
            .build();

    new Runner(opt).run();
    /*VectorAPIBenchmarks vab = new VectorAPIBenchmarks();
    vab.prepare();
    vab.testSafe();
    vab.testUnSafe();
    vab.tearDown();*/
  }
}
