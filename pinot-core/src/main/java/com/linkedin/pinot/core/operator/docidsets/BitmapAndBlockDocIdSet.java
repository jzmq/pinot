/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.docidsets;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.utils.Pairs;
import com.linkedin.pinot.core.common.*;
import com.linkedin.pinot.core.operator.filter.AndOperator;
import com.linkedin.pinot.core.operator.filter.utils.BitmapUtils;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;


public final class BitmapAndBlockDocIdSet implements FilterBlockDocIdSet {
  /**
   *
   */
//  private final BlockDocIdIterator[] docIdIterators;
//  private final int[] docIdPointers;
  private static final Logger LOGGER = LoggerFactory.getLogger(AndOperator.class);
  //  boolean reachedEnd = false;
//  int currentDocId = -1;
  public final AtomicLong timeMeasure = new AtomicLong(0);
  private List<FilterBlockDocIdSet> blockDocIdSets;
  private int minDocId = Integer.MIN_VALUE;
  private int maxDocId = Integer.MAX_VALUE;

  BitmapBasedBlockIdSetIterator bitmapBasedBlockIdSetIterator;
  MutableRoaringBitmap mergedBitmap;

  public BitmapAndBlockDocIdSet(List<FilterBlockDocIdSet> blockDocIdSets) {
    this.blockDocIdSets = blockDocIdSets;
//    final int[] docIdPointers = new int[blockDocIdSets.size()];
//    final BlockDocIdIterator[] docIdIterators = new BlockDocIdIterator[blockDocIdSets.size()];
//    for (int srcId = 0; srcId < blockDocIdSets.size(); srcId++) {
//      docIdIterators[srcId] = blockDocIdSets.get(srcId).iterator();
//    }
//    Arrays.fill(docIdPointers, -1);
//    this.docIdIterators = docIdIterators;
//    this.docIdPointers = docIdPointers;


    ImmutableRoaringBitmap[] btList = new ImmutableRoaringBitmap[blockDocIdSets.size()];
    ImmutableRoaringBitmap[] tmpList;
    List<IntIterator> iteratorList = new ArrayList<>();
    for (int i = 0; i < blockDocIdSets.size(); i++) {
      tmpList = blockDocIdSets.get(i).getRaw();
      btList[i] = tmpList[0];
//      btList[i] = ((BitmapDocIdSet)(blockDocIdSets.get(i))).getRaw();
    }
    mergedBitmap = BitmapUtils.fastBitmapsAnd(btList);

    
//    IntIterator[] iterators = {mergedBitmap.getIntIterator()};
    bitmapBasedBlockIdSetIterator = new BitmapBasedBlockIdSetIterator(iteratorList.toArray(new
            IntIterator[iteratorList.size()]));
    updateMinMaxRange();
  }

  private void updateMinMaxRange() {
    for (FilterBlockDocIdSet blockDocIdSet : blockDocIdSets) {
      minDocId = Math.max(minDocId, blockDocIdSet.getMinDocId());
      maxDocId = Math.min(maxDocId, blockDocIdSet.getMaxDocId());
    }
    bitmapBasedBlockIdSetIterator.setStartDocId(minDocId);
    bitmapBasedBlockIdSetIterator.setEndDocId(maxDocId);
//    for (FilterBlockDocIdSet blockDocIdSet : blockDocIdSets) {
//      blockDocIdSet.setStartDocId(minDocId);
//      blockDocIdSet.setEndDocId(maxDocId);
//      bitmapBasedBlockIdSetIterator.setStartDocId(minDocId);
//      bitmapBasedBlockIdSetIterator.setEndDocId(minDocId);
//    }
  }

  @Override
  public BlockDocIdIterator iterator() {
    return bitmapBasedBlockIdSetIterator;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getRaw() {
    return (T) this.blockDocIdSets;
  }

  @Override
  public int getMinDocId() {
    return minDocId;
  }

  @Override
  public int getMaxDocId() {
    return maxDocId;
  }

  @Override
  public void setStartDocId(int startDocId) {
    minDocId = Math.max(minDocId, startDocId);
    updateMinMaxRange();
  }

  @Override
  public void setEndDocId(int endDocId) {
    maxDocId = Math.min(maxDocId, endDocId);
    updateMinMaxRange();
  }

  public final class BitmapBasedBlockIdSetIterator implements BlockDocIdIterator {
    final private IntIterator[] iterators;
    private int endDocId;
    private int startDocId;

    public BitmapBasedBlockIdSetIterator(IntIterator[] iterators) {
      this.iterators = iterators;
    }

    public void setEndDocId(int endDocId) {
      this.endDocId = endDocId;
    }

    public void setStartDocId(int startDocId) {
      this.startDocId = startDocId;
    }

    //<docId Counter, postinglist Id> Int Pair
    PriorityQueue<Pairs.IntPair> queue = new PriorityQueue<Pairs.IntPair>(1, new Pairs
            .AscendingIntPairComparator());
    boolean[] iteratorIsInQueue = new boolean[1];
    int currentDocId = -1;

    @Override
    public int advance(int targetDocId) {
      long start = System.nanoTime();
      if (targetDocId < startDocId) {
        targetDocId = startDocId;
      } else if (targetDocId > endDocId) {
        currentDocId = Constants.EOF;
      }
      if (currentDocId == Constants.EOF) {
        return currentDocId;
      }
      Iterator<Pairs.IntPair> iterator = queue.iterator();
      //remove everything from the queue that is less than targetDocId
      while (iterator.hasNext()) {
        Pairs.IntPair pair = iterator.next();
        if (pair.getA() < targetDocId) {
          iterator.remove();
          iteratorIsInQueue[pair.getB()] = false;
        }
      }
      //move the pointer until its great than or equal to targetDocId
      for (int i = 0; i < iterators.length; i++) {
        if (!iteratorIsInQueue[i]) {
          int next;
          while (iterators[i].hasNext()) {
            next = iterators[i].next();
            if (next > endDocId) {
              break;
            }
            if (next >= targetDocId) {
              queue.add(new Pairs.IntPair(next, i));
              break;
            }
          }
          iteratorIsInQueue[i] = true;
        }
      }
      if (queue.size() > 0) {
        currentDocId = queue.peek().getA();
      } else {
        currentDocId = Constants.EOF;
      }
      long end = System.nanoTime();
      timeMeasure.addAndGet(end - start);
      return currentDocId;
    }

    @Override
    public int next() {
      long start = System.nanoTime();
      if (currentDocId == Constants.EOF) {
        return currentDocId;
      }
      while (queue.size() > 0 && queue.peek().getA() <= currentDocId) {
        Pairs.IntPair pair = queue.remove();
        iteratorIsInQueue[pair.getB()] = false;
      }
      currentDocId++;
      for (int i = 0; i < iterators.length; i++) {
        if (!iteratorIsInQueue[i]) {
          while (iterators[i].hasNext()) {
            int next = iterators[i].next();
            if (next >= startDocId && next <= endDocId && next >= currentDocId) {
              queue.add(new Pairs.IntPair(next, i));
              break;
            }
            if (next > endDocId) {
              break;
            }
          }
          iteratorIsInQueue[i] = true;
        }
      }
      if (queue.size() > 0) {
        currentDocId = queue.peek().getA();
      } else {
        currentDocId = Constants.EOF;
      }
      long end = System.nanoTime();
      timeMeasure.addAndGet(end - start);
      return currentDocId;
    }

    @Override
    public int currentDocId() {
      return currentDocId;
    }
  }

}
