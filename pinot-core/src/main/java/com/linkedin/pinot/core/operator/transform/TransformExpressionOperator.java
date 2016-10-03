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
package com.linkedin.pinot.core.operator.transform;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.operator.docidsets.DocIdSetBlock;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;


/**
 * Class for evaluating transform expressions.
 */
public class TransformExpressionOperator extends BaseOperator {
  private static final String OPERATOR_NAME = "TransformExpressionOperator";

  private final IndexSegment _indexSegment;
  private final MProjectionOperator _projectionOperator;
  TransformExpressionEvaluator _expressionEvaluator;
  private int _nextBlockCallCounter = 0;

  // Reusable thread-local int array to store the doc ids.
  private static final ThreadLocal<int[]> REUSABLE_DOC_ID_SET = new ThreadLocal<int[]>() {
    @Override
    protected int[] initialValue() {
      return new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
  };

  /**
   * Constructor for the class
   *
   * @param indexSegment Segment to process
   * @param projectionOperator Projection operator
   * @param expressionTree Expression tree to evaluate
   */
  public TransformExpressionOperator(IndexSegment indexSegment, MProjectionOperator projectionOperator,
      TransformExpressionTree expressionTree) {

    Preconditions.checkArgument((indexSegment != null));
    Preconditions.checkArgument((projectionOperator != null));
    Preconditions.checkArgument((expressionTree != null));

    _indexSegment = indexSegment;
    _projectionOperator = projectionOperator;

    _expressionEvaluator = new DefaultExpressionEvaluator(indexSegment, expressionTree);
  }

  @Override
  public Block getNextBlock() {
    return getNextBlock(new BlockId(_nextBlockCallCounter++));
  }

  @Override
  public Block getNextBlock(BlockId blockId) {
    // Only one block supported, currently.
    if (blockId.getId() > 0) {
      return null;
    }

    int numDocsScanned = 0;
    final long startTimeMillis = System.currentTimeMillis();

    _expressionEvaluator.init();
    while (_projectionOperator.nextBlock() != null) {
      ProjectionBlock currentBlock = _projectionOperator.getCurrentBlock();
      Block docIdSetBlock = currentBlock.getDocIdSetBlock();
      numDocsScanned = processBlock(currentBlock, docIdSetBlock, numDocsScanned);
    }
    _expressionEvaluator.finish();

    IntermediateResultsBlock result = new IntermediateResultsBlock(_expressionEvaluator.getResult());
    result.setTotalRawDocs(_indexSegment.getSegmentMetadata().getTotalRawDocs());
    result.setTimeUsedMs(System.currentTimeMillis() - startTimeMillis);

    return result;
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public boolean open() {
    return _projectionOperator.open();
  }

  @Override
  public boolean close() {
    return _projectionOperator.close();
  }

  /**
   * Process a block of docIdSets by evaluating the transform expression on each doc.
   *
   * @param projectionBlock Project block to process
   * @param block DocIdSet block to process
   * @param numDocsScanned Number of documents already scanned
   * @return Number of documents scanned (including the original passed-in value)
   */
  private int processBlock(ProjectionBlock projectionBlock, Block block, int numDocsScanned) {

    if (block instanceof DocIdSetBlock) {
      DocIdSetBlock docIdSetBlock = (DocIdSetBlock) block;
      int length = docIdSetBlock.getSearchableLength();

      _expressionEvaluator.evaluate(docIdSetBlock.getDocIdSet(), length);
      numDocsScanned += length;

    } else {

      BlockDocIdSet blockDocIdSet = projectionBlock.getBlockDocIdSet();
      BlockDocIdIterator iterator = blockDocIdSet.iterator();

      int docId;
      int position = 0;
      int[] docIds = REUSABLE_DOC_ID_SET.get();

      while ((docId = iterator.next()) != Constants.EOF) {
        docIds[position++] = docId;
        if (position == DocIdSetPlanNode.MAX_DOC_PER_CALL) {
          numDocsScanned += position;
          _expressionEvaluator.evaluate(docIds, position);
          position = 0;
        }
      }

      if (position > 0) {
        _expressionEvaluator.evaluate(docIds, position);
        numDocsScanned += position;
      }
    }

    return numDocsScanned;
  }
}
