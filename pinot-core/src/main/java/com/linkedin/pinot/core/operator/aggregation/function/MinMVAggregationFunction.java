/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.aggregation.function;

import java.util.List;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.operator.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.operator.aggregation.groupby.GroupByResultHolder;


/**
 * Class to implement the 'min' aggregation function.
 */
public class MinMVAggregationFunction implements AggregationFunction {
  private static final String FUNCTION_NAME = AggregationFunctionFactory.MIN_MV_AGGREGATION_FUNCTION;
  private static final double DEFAULT_VALUE = Double.POSITIVE_INFINITY;
  private static final ResultDataType RESULT_DATA_TYPE = ResultDataType.DOUBLE;

  /**
   * Performs 'min' aggregation function on the input array.
   * Returns {@value #DEFAULT_VALUE} if input array is empty.
   *
   * While the interface allows for variable number of valueArrays, we do not support
   * multiple columns within one aggregation function right now.
   *
   * {@inheritDoc}
   *
   * @param length
   * @param resultHolder
   * @param valueArrayArray
   */
  @Override
  public void aggregate(int length, AggregationResultHolder resultHolder, Object... valueArrayArray) {
    Preconditions.checkArgument(valueArrayArray.length == 1);
    Preconditions.checkArgument(valueArrayArray[0] instanceof double[][]);
    final double[][] values = (double[][]) valueArrayArray[0];
    Preconditions.checkState(length <= values.length);

    double min = DEFAULT_VALUE;
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < values[i].length; ++j) {
        if (values[i][j] < min) {
          min = values[i][j];
        }
      }
    }
    double oldValue = resultHolder.getDoubleResult();
    if (min < oldValue) {
      resultHolder.setValue(min);
    }
  }

  /**
   * {@inheritDoc}
   * While the interface allows for variable number of valueArrays, we do not support
   * multiple columns within one aggregation function right now.
   *
   * @param length
   * @param groupKeys
   * @param resultHolder
   * @param valueArray
   */
  @Override
  public void aggregateGroupBySV(int length, int[] groupKeys, GroupByResultHolder resultHolder, Object... valueArray) {
    Preconditions.checkArgument(valueArray.length == 1);
    Preconditions.checkArgument(valueArray[0] instanceof double[][]);
    final double[][] values = (double[][]) valueArray[0];
    Preconditions.checkState(length <= values.length);
    for (int i = 0; i < length; i++) {
      int groupKey = groupKeys[i];
      double oldValue = resultHolder.getDoubleResult(groupKey);
      for (double value : values[i]) {
        if (value < oldValue) {
          resultHolder.setValueForKey(groupKey, value);
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * @param length
   * @param docIdToGroupKeys
   * @param resultHolder
   * @param valueArray
   */
  @Override
  public void aggregateGroupByMV(int length, int[][] docIdToGroupKeys, GroupByResultHolder resultHolder,
      Object... valueArray) {
    Preconditions.checkArgument(valueArray.length == 1);
    Preconditions.checkArgument(valueArray[0] instanceof double[][]);
    final double[][] values = (double[][]) valueArray[0];
    Preconditions.checkState(length <= values.length);
    for (int i = 0; i < length; ++i) {
      for (int groupKey : docIdToGroupKeys[i]) {
        double oldValue = resultHolder.getDoubleResult(groupKey);
        for (double value : values[i]) {
          if (value < oldValue) {
            resultHolder.setValueForKey(groupKey, value);
          }
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * @param combinedResult
   * @return
   */
  @Override
  public Double reduce(List<Object> combinedResult) {
    double reducedResult = DEFAULT_VALUE;

    for (Object object : combinedResult) {
      double result = (Double) object;
      if (result < reducedResult) {
        reducedResult = result;
      }
    }
    return reducedResult;
  }

  /**
   * {@inheritDoc}
   *
   * @return
   */
  @Override
  public double getDefaultValue() {
    return DEFAULT_VALUE;
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public ResultDataType getResultDataType() {
    return RESULT_DATA_TYPE;
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public String getName() {
    return FUNCTION_NAME;
  }
}
