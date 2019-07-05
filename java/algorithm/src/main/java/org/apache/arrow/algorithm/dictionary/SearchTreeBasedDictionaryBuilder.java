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

import java.util.TreeSet;

import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.ValueVector;

/**
 * The dictionary based on a search tree.
 * So each add operation can be finished in O(log(n)),
 * where n is the current dictionary size.
 * @param <V> the dictionary vector type.
 */
public class SearchTreeBasedDictionaryBuilder<V extends ValueVector> extends DictionaryBuilder<V> {

  private final BaseFixedWidthVector fixedWidthVector;

  private final BaseVariableWidthVector variableWidthVector;

  /**
   * The search tree for storing the value index.
   */
  protected TreeSet<Integer> searchTree =
          new TreeSet<>((index1, index2) -> comparator.compare(index1, index2));

  /**
   * Construct a search tree-based dictionary builder.
   * @param dictionary the dictionary vector.
   * @param comparator the criteria for value equality.
   */
  public SearchTreeBasedDictionaryBuilder(V dictionary, VectorValueComparator<V> comparator) {
    super(dictionary, comparator);

    if (dictionary instanceof BaseFixedWidthVector) {
      fixedWidthVector = (BaseFixedWidthVector) dictionary;
      variableWidthVector = null;
    } else if (dictionary instanceof BaseVariableWidthVector) {
      fixedWidthVector = null;
      variableWidthVector = (BaseVariableWidthVector) dictionary;
    } else {
      throw new IllegalArgumentException("The dictionary must be a BaseFixedWidthVector or a BaseVariableWidthVector");
    }
  }

  @Override
  public boolean addValue(V targetVector, int targetIndex) {
    totalValueCount += 1;

    // first copy the value to the end of the dictionary
    if (fixedWidthVector != null) {
      fixedWidthVector.copyFromSafe(targetIndex, distinctValueCount, (BaseFixedWidthVector) targetVector);
    } else {
      variableWidthVector.copyFromSafe(targetIndex, distinctValueCount, (BaseVariableWidthVector) targetVector);
    }

    boolean ret = searchTree.add(distinctValueCount);
    distinctValueCount = searchTree.size();
    return ret;
  }

  /**
   * Gets the sorted dictionary.
   * Note that given the binary search tree, the sort can finish in O(n).
   */
  public void getSortedDictionary(V sortedDictionary) {
    if (fixedWidthVector != null) {
      BaseFixedWidthVector fixedDict = (BaseFixedWidthVector) sortedDictionary;
      int idx = 0;
      for (Integer dictIdx : searchTree) {
        fixedDict.copyFromSafe(dictIdx, idx++, fixedWidthVector);
      }
    } else {
      BaseVariableWidthVector variableDict = (BaseVariableWidthVector) sortedDictionary;
      int idx = 0;
      for (Integer dictIdx : searchTree) {
        variableDict.copyFromSafe(dictIdx, idx++, variableWidthVector);
      }
    }
    sortedDictionary.setValueCount(distinctValueCount);
  }
}
