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

package org.apache.arrow.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableUInt4Holder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

public class TestDenseUnionVector {
  private static final String EMPTY_SCHEMA_PATH = "";

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testDenseUnionVector() throws Exception {

    final NullableUInt4Holder uInt4Holder = new NullableUInt4Holder();
    uInt4Holder.value = 100;
    uInt4Holder.isSet = 1;

    try (DenseUnionVector unionVector = new DenseUnionVector(EMPTY_SCHEMA_PATH, allocator, null, null)) {
      unionVector.allocateNew();

      // write some data
      unionVector.setType(0, MinorType.UINT4);
      unionVector.setSafe(0, uInt4Holder);
      unionVector.setType(2, MinorType.UINT4);
      unionVector.setSafe(2, uInt4Holder);
      unionVector.setValueCount(4);

      // check that what we wrote is correct
      assertEquals(4, unionVector.getValueCount());

      assertEquals(false, unionVector.isNull(0));
      assertEquals(100, unionVector.getObject(0));

      assertEquals(true, unionVector.isNull(1));

      assertEquals(false, unionVector.isNull(2));
      assertEquals(100, unionVector.getObject(2));

      assertEquals(true, unionVector.isNull(3));
    }
  }

  @Test
  public void testTransfer() throws Exception {
    try (DenseUnionVector srcVector = new DenseUnionVector(EMPTY_SCHEMA_PATH, allocator, null, null)) {
      srcVector.allocateNew();

      // write some data
      srcVector.setType(0, MinorType.INT);
      srcVector.setSafe(0, newIntHolder(5));
      srcVector.setType(1, MinorType.BIT);
      srcVector.setSafe(1, newBitHolder(false));
      srcVector.setType(3, MinorType.INT);
      srcVector.setSafe(3, newIntHolder(10));
      srcVector.setType(5, MinorType.BIT);
      srcVector.setSafe(5, newBitHolder(false));
      srcVector.setValueCount(6);

      try (DenseUnionVector destVector = new DenseUnionVector(EMPTY_SCHEMA_PATH, allocator, null, null)) {
        TransferPair pair = srcVector.makeTransferPair(destVector);

        // Creating the transfer should transfer the type of the field at least.
        assertEquals(srcVector.getField(), destVector.getField());

        // transfer
        pair.transfer();

        assertEquals(srcVector.getField(), destVector.getField());

        // now check the values are transferred
        assertEquals(6, destVector.getValueCount());

        assertFalse(destVector.isNull(0));
        assertEquals(5, destVector.getObject(0));

        assertFalse(destVector.isNull(1));
        assertEquals(false, destVector.getObject(1));

        assertTrue(destVector.isNull(2));

        assertFalse(destVector.isNull(3));
        assertEquals(10, destVector.getObject(3));

        assertTrue(destVector.isNull(4));

        assertFalse(destVector.isNull(5));
        assertEquals(false, destVector.getObject(5));
      }
    }
  }

  @Test
  public void testSplitAndTransfer() throws Exception {
    try (DenseUnionVector sourceVector = new DenseUnionVector(EMPTY_SCHEMA_PATH, allocator, null, null)) {

      sourceVector.allocateNew();

      /* populate the UnionVector */
      sourceVector.setType(0, MinorType.INT);
      sourceVector.setSafe(0, newIntHolder(5));
      sourceVector.setType(1, MinorType.INT);
      sourceVector.setSafe(1, newIntHolder(10));
      sourceVector.setType(2, MinorType.INT);
      sourceVector.setSafe(2, newIntHolder(15));
      sourceVector.setType(3, MinorType.INT);
      sourceVector.setSafe(3, newIntHolder(20));
      sourceVector.setType(4, MinorType.INT);
      sourceVector.setSafe(4, newIntHolder(25));
      sourceVector.setType(5, MinorType.INT);
      sourceVector.setSafe(5, newIntHolder(30));
      sourceVector.setType(6, MinorType.INT);
      sourceVector.setSafe(6, newIntHolder(35));
      sourceVector.setType(7, MinorType.INT);
      sourceVector.setSafe(7, newIntHolder(40));
      sourceVector.setType(8, MinorType.INT);
      sourceVector.setSafe(8, newIntHolder(45));
      sourceVector.setType(9, MinorType.INT);
      sourceVector.setSafe(9, newIntHolder(50));
      sourceVector.setValueCount(10);

      /* check the vector output */
      assertEquals(10, sourceVector.getValueCount());
      assertEquals(false, sourceVector.isNull(0));
      assertEquals(5, sourceVector.getObject(0));
      assertEquals(false, sourceVector.isNull(1));
      assertEquals(10, sourceVector.getObject(1));
      assertEquals(false, sourceVector.isNull(2));
      assertEquals(15, sourceVector.getObject(2));
      assertEquals(false, sourceVector.isNull(3));
      assertEquals(20, sourceVector.getObject(3));
      assertEquals(false, sourceVector.isNull(4));
      assertEquals(25, sourceVector.getObject(4));
      assertEquals(false, sourceVector.isNull(5));
      assertEquals(30, sourceVector.getObject(5));
      assertEquals(false, sourceVector.isNull(6));
      assertEquals(35, sourceVector.getObject(6));
      assertEquals(false, sourceVector.isNull(7));
      assertEquals(40, sourceVector.getObject(7));
      assertEquals(false, sourceVector.isNull(8));
      assertEquals(45, sourceVector.getObject(8));
      assertEquals(false, sourceVector.isNull(9));
      assertEquals(50, sourceVector.getObject(9));

      try (DenseUnionVector toVector = new DenseUnionVector(EMPTY_SCHEMA_PATH, allocator, null, null)) {

        final TransferPair transferPair = sourceVector.makeTransferPair(toVector);

        final int[][] transferLengths = {{0, 3},
          {3, 1},
          {4, 2},
          {6, 1},
          {7, 1},
          {8, 2}
        };

        for (final int[] transferLength : transferLengths) {
          final int start = transferLength[0];
          final int length = transferLength[1];

          transferPair.splitAndTransfer(start, length);

          /* check the toVector output after doing the splitAndTransfer */
          for (int i = 0; i < length; i++) {
            assertEquals("Different data at indexes: " + (start + i) + "and " + i, sourceVector.getObject(start + i),
                    toVector.getObject(i));
          }
        }
      }
    }
  }

  @Test
  public void testSplitAndTransferWithMixedVectors() throws Exception {
    try (DenseUnionVector sourceVector = new DenseUnionVector(EMPTY_SCHEMA_PATH, allocator, null, null)) {

      sourceVector.allocateNew();

      /* populate the UnionVector */
      sourceVector.setType(0, MinorType.INT);
      sourceVector.setSafe(0, newIntHolder(5));

      sourceVector.setType(1, MinorType.FLOAT4);
      sourceVector.setSafe(1, newFloat4Holder(5.5f));

      sourceVector.setType(2, MinorType.INT);
      sourceVector.setSafe(2, newIntHolder(10));

      sourceVector.setType(3, MinorType.FLOAT4);
      sourceVector.setSafe(3, newFloat4Holder(10.5f));

      sourceVector.setType(4, MinorType.INT);
      sourceVector.setSafe(4, newIntHolder(15));

      sourceVector.setType(5, MinorType.FLOAT4);
      sourceVector.setSafe(5, newFloat4Holder(15.5f));

      sourceVector.setType(6, MinorType.INT);
      sourceVector.setSafe(6, newIntHolder(20));

      sourceVector.setType(7, MinorType.FLOAT4);
      sourceVector.setSafe(7, newFloat4Holder(20.5f));

      sourceVector.setType(8, MinorType.INT);
      sourceVector.setSafe(8, newIntHolder(30));

      sourceVector.setType(9, MinorType.FLOAT4);
      sourceVector.setSafe(9, newFloat4Holder(30.5f));
      sourceVector.setValueCount(10);

      /* check the vector output */
      assertEquals(10, sourceVector.getValueCount());
      assertEquals(false, sourceVector.isNull(0));
      assertEquals(5, sourceVector.getObject(0));
      assertEquals(false, sourceVector.isNull(1));
      assertEquals(5.5f, sourceVector.getObject(1));
      assertEquals(false, sourceVector.isNull(2));
      assertEquals(10, sourceVector.getObject(2));
      assertEquals(false, sourceVector.isNull(3));
      assertEquals(10.5f, sourceVector.getObject(3));
      assertEquals(false, sourceVector.isNull(4));
      assertEquals(15, sourceVector.getObject(4));
      assertEquals(false, sourceVector.isNull(5));
      assertEquals(15.5f, sourceVector.getObject(5));
      assertEquals(false, sourceVector.isNull(6));
      assertEquals(20, sourceVector.getObject(6));
      assertEquals(false, sourceVector.isNull(7));
      assertEquals(20.5f, sourceVector.getObject(7));
      assertEquals(false, sourceVector.isNull(8));
      assertEquals(30, sourceVector.getObject(8));
      assertEquals(false, sourceVector.isNull(9));
      assertEquals(30.5f, sourceVector.getObject(9));

      try (DenseUnionVector toVector = new DenseUnionVector(EMPTY_SCHEMA_PATH, allocator, null, null)) {

        final TransferPair transferPair = sourceVector.makeTransferPair(toVector);

        final int[][] transferLengths = {{0, 2},
            {2, 1},
            {3, 2},
            {5, 3},
            {8, 2}
        };

        for (final int[] transferLength : transferLengths) {
          final int start = transferLength[0];
          final int length = transferLength[1];

          transferPair.splitAndTransfer(start, length);

          /* check the toVector output after doing the splitAndTransfer */
          for (int i = 0; i < length; i++) {
            assertEquals("Different values at index: " + i, sourceVector.getObject(start + i), toVector.getObject(i));
          }
        }
      }
    }
  }

  @Test
  public void testGetFieldTypeInfo() throws Exception {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("key1", "value1");

    int[] typeIds = new int[2];
    typeIds[0] = MinorType.INT.ordinal();
    typeIds[1] = MinorType.VARCHAR.ordinal();

    List<Field> children = new ArrayList<>();
    children.add(new Field("int", FieldType.nullable(MinorType.INT.getType()), null));
    children.add(new Field("varchar", FieldType.nullable(MinorType.VARCHAR.getType()), null));

    final FieldType fieldType = new FieldType(false, new ArrowType.Union(UnionMode.Dense, typeIds),
            /*dictionary=*/null, metadata);
    final Field field = new Field("union", fieldType, children);

    MinorType minorType = MinorType.UNION;
    DenseUnionVector vector = (DenseUnionVector) minorType.getNewVector(field, allocator, null);
    vector.initializeChildrenFromFields(children);

    assertTrue(vector.getField().equals(field));
  }

  @Test
  public void testGetBufferAddress() throws Exception {
    try (DenseUnionVector vector = new DenseUnionVector(EMPTY_SCHEMA_PATH, allocator, null, null)) {
      boolean error = false;

      vector.allocateNew();

      /* populate the UnionVector */
      vector.setType(0, MinorType.INT);
      vector.setSafe(0, newIntHolder(5));

      vector.setType(1, MinorType.FLOAT4);
      vector.setSafe(1, newFloat4Holder(5.5f));

      vector.setType(2, MinorType.INT);
      vector.setSafe(2, newIntHolder(10));

      vector.setType(3, MinorType.FLOAT4);
      vector.setSafe(3, newFloat4Holder(10.5f));

      vector.setValueCount(10);

      /* check the vector output */
      assertEquals(10, vector.getValueCount());
      assertEquals(false, vector.isNull(0));
      assertEquals(5, vector.getObject(0));
      assertEquals(false, vector.isNull(1));
      assertEquals(5.5f, vector.getObject(1));
      assertEquals(false, vector.isNull(2));
      assertEquals(10, vector.getObject(2));
      assertEquals(false, vector.isNull(3));
      assertEquals(10.5f, vector.getObject(3));

      List<ArrowBuf> buffers = vector.getFieldBuffers();

      long bitAddress = vector.getValidityBufferAddress();
      long offsetAddress = vector.getOffsetBufferAddress();

      try {
        long dataAddress = vector.getDataBufferAddress();
      } catch (UnsupportedOperationException ue) {
        error = true;
      } finally {
        assertTrue(error);
      }

      assertEquals(2, buffers.size());
      assertEquals(bitAddress, buffers.get(0).memoryAddress());
      assertEquals(offsetAddress, buffers.get(1).memoryAddress());
    }
  }

  /**
   * Test adding two struct vectors to the dense union vector.
   */
  @Test
  public void testMultipleStructs() {
    FieldType type = new FieldType(true, ArrowType.Struct.INSTANCE, null, null);
    try (StructVector structVector1 = new StructVector("struct", allocator, type, null);
         StructVector structVector2 = new StructVector("struct", allocator, type, null);
         DenseUnionVector unionVector = DenseUnionVector.empty("union", allocator)) {

      // prepare sub vectors

      // first struct vector: (int, int)
      IntVector subVector11 = structVector1
              .addOrGet("sub11", FieldType.nullable(MinorType.INT.getType()), IntVector.class);
      subVector11.allocateNew();
      subVector11.setSafe(0, 0);
      subVector11.setSafe(1, 1);
      subVector11.setValueCount(2);

      IntVector subVector12 =  structVector1
              .addOrGet("sub12", FieldType.nullable(MinorType.INT.getType()), IntVector.class);
      subVector12.allocateNew();
      subVector12.setSafe(0, 0);
      subVector12.setSafe(1, 10);
      subVector12.setValueCount(2);

      structVector1.setIndexDefined(0);
      structVector1.setIndexDefined(1);
      structVector1.setValueCount(2);

      // second struct vector: (string, string)
      VarCharVector subVector21 = structVector2
              .addOrGet("sub21", FieldType.nullable(MinorType.VARCHAR.getType()), VarCharVector.class);
      subVector21.allocateNew();
      subVector21.setSafe(0, "a0".getBytes());
      subVector21.setValueCount(1);

      VarCharVector subVector22 = structVector2
              .addOrGet("sub22", FieldType.nullable(MinorType.VARCHAR.getType()), VarCharVector.class);
      subVector22.allocateNew();
      subVector22.setSafe(0, "b0".getBytes());
      subVector22.setValueCount(1);

      structVector2.setIndexDefined(0);
      structVector2.setValueCount(1);

      // register relative types
      int typeId1 = unionVector.getOrAllocateTypeId(structVector1.getField());
      int typeId2 = unionVector.getOrAllocateTypeId(structVector2.getField());
      assertEquals(typeId1, MinorType.values().length);
      assertEquals(typeId2, MinorType.values().length + 1);

      // add two struct vectors to union vector
      unionVector.addVector(structVector1);
      unionVector.addVector(structVector2);
      unionVector.setValueCount(3);

      ArrowBuf offsetBuf = unionVector.getOffsetBuffer();

      unionVector.setType(0, typeId1);
      offsetBuf.setInt(0, 0);
      BitVectorHelper.setValidityBitToOne(unionVector.getValidityBuffer(), 0);

      unionVector.setType(1, typeId2);
      offsetBuf.setInt(DenseUnionVector.OFFSET_WIDTH, 0);
      BitVectorHelper.setValidityBitToOne(unionVector.getValidityBuffer(), 1);

      unionVector.setType(2, typeId1);
      offsetBuf.setInt(DenseUnionVector.OFFSET_WIDTH * 2, 1);
      BitVectorHelper.setValidityBitToOne(unionVector.getValidityBuffer(), 2);

      Map<String, Integer> value0 = new JsonStringHashMap<>();
      value0.put("sub11", 0);
      value0.put("sub12", 0);

      assertEquals(value0, unionVector.getObject(0));

      Map<String, Text> value1 = new JsonStringHashMap<>();
      value1.put("sub21", new Text("a0"));
      value1.put("sub22", new Text("b0"));

      assertEquals(value1, unionVector.getObject(1));

      Map<String, Integer> value2 = new JsonStringHashMap<>();
      value2.put("sub11", 1);
      value2.put("sub12", 10);

      assertEquals(value2, unionVector.getObject(2));
    }
  }

  private static NullableIntHolder newIntHolder(int value) {
    final NullableIntHolder holder = new NullableIntHolder();
    holder.isSet = 1;
    holder.value = value;
    return holder;
  }

  private static NullableBitHolder newBitHolder(boolean value) {
    final NullableBitHolder holder = new NullableBitHolder();
    holder.isSet = 1;
    holder.value = value ? 1 : 0;
    return holder;
  }

  private static NullableFloat4Holder newFloat4Holder(float value) {
    final NullableFloat4Holder holder = new NullableFloat4Holder();
    holder.isSet = 1;
    holder.value = value;
    return holder;
  }
}
