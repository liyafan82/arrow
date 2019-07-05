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

package org.apache.arrow.algorithm.dictionary;

import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.vector.ValueVector;

/**
 * The class for building a dictionary.
 * It is intended for the scenario which can be frequently encountered in practice:
 * the dictionary is not known a priori, so it is generated dynamically.
 * In particular, when a new value arrives, it is tested to check if it is already
 * in the dictionary. If so, it is simply neglected, otherwise, it is added to the dictionary.
 *
 * <p>
 *   When all values have been evaluated, the dictionary can be considered complete.
 *   So encoding can start afterward.
 * </p>
 * @param <V> the dictionary vector type.
 */
public abstract class DictionaryBuilder<V extends ValueVector> {

  protected final V dictionary;

  protected final VectorValueComparator<V> comparator;

  protected int totalValueCount;

  protected int distinctValueCount;

  /**
   * Construct a dictionary builder.
   * @param dictionary the dictionary vector.
   * @param comparator the criteria for value equality.
   */
  public DictionaryBuilder(V dictionary, VectorValueComparator<V> comparator) {
    this.dictionary = dictionary;
    this.comparator = comparator;
    this.comparator.attachVector(dictionary);
  }

  public void startBuild() {
    totalValueCount = 0;
    distinctValueCount = 0;
  }

  public void endBuild() {
    dictionary.setValueCount(distinctValueCount);
  }

  public V getDictionary() {
    return dictionary;
  }

  /**
   * Try to add all values from the target vector to the dictionary.
   * @param targetVector the target vector containing new values.
   * @return the number of values actually added to the dictionary.
   */
  public int addValues(V targetVector) {
    int ret = 0;
    for (int i = 0; i < targetVector.getValueCount(); i++) {
      if (addValue(targetVector, i)) {
        ret += 1;
      }
    }
    return ret;
  }

  /**
   * Try to add a new value to the dictionary.
   * @param targetVector the vector that contains the new value.
   * @param targetIndex the index of the new value in the target vector.
   * @return true if the value is actually added, and false otherwise.
   */
  public abstract boolean addValue(V targetVector, int targetIndex);
}
