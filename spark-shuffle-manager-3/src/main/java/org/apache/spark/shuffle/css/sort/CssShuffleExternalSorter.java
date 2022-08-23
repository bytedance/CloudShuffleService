/*
 * This file may have been modified by Bytedance Inc.
 *
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

package org.apache.spark.shuffle.css.sort;

import com.bytedance.css.common.protocol.PartitionGroupManager;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.TooLargePageException;
import org.apache.spark.shuffle.css.AsyncPushDataTaskManager;
import org.apache.spark.shuffle.css.ShuffleWriteMetricsAdapter;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * An external sorter that is specialized for sort-based shuffle.
 * <p>
 * Incoming records are appended to data pages. When all records have been inserted (or when the
 * current thread's shuffle memory limit is reached), the in-memory records are sorted according to
 * their partition ids (using a {@link ShuffleInMemorySorter}). The sorted records are then
 * written to a single output file (or multiple files, if we've spilled). The format of the output
 * files is the same as the format of the final output file written by
 * {@link org.apache.spark.shuffle.sort.SortShuffleWriter}: each output partition's records are
 * written as a single serialized, compressed stream that can be read with a new decompression and
 * deserialization stream.
 * <p>
 * Unlike {@link org.apache.spark.util.collection.ExternalSorter}, this sorter does not merge its
 * spill files. Instead, this merging is performed in UnsafeShuffleWriter, which uses a
 * specialized merge procedure that avoids extra serialization/deserialization.
 */
public final class CssShuffleExternalSorter extends MemoryConsumer {

  private static final Logger logger = LoggerFactory.getLogger(CssShuffleExternalSorter.class);

  private final TaskMemoryManager taskMemoryManager;
  private final TaskContext taskContext;
  private final AsyncPushDataTaskManager taskManager;
  private final ShuffleWriteMetricsAdapter writeMetrics;

  private final int PARTITION_GROUP_PUSH_BUFFER_SIZE;
  private final PartitionGroupManager partitionGroupManager;

  /**
   * Memory pages that hold the records being sorted. The pages in this list are freed when
   * spilling, although in principle we could recycle these pages across spills (on the other hand,
   * this might not be necessary if we maintained a pool of re-usable pages in the TaskMemoryManager
   * itself).
   */
  private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<>();

  /** Peak memory used by this sorter so far, in bytes. **/
  private long peakMemoryUsedBytes;

  // These variables are reset after spilling:
  @Nullable private ShuffleInMemorySorter inMemSorter;
  @Nullable private MemoryBlock currentPage = null;
  private long pageCursor = -1;
  private long spillTimes = 0L;

  private final long sortPushSpillSizeThreshold;
  private final long sortPushSpillRecordThreshold;

  public CssShuffleExternalSorter(
      TaskMemoryManager memoryManager,
      TaskContext taskContext,
      AsyncPushDataTaskManager taskManager,
      long sortPushSpillSizeThreshold,
      long sortPushSpillRecordThreshold,
      int initialSize,
      int partitionGroupPushBufferSize,
      PartitionGroupManager partitionGroupManager,
      SparkConf conf,
      ShuffleWriteMetricsAdapter writeMetrics) {
    super(memoryManager,
      (int) Math.min(PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES, memoryManager.pageSizeBytes()),
      memoryManager.getTungstenMemoryMode());
    this.taskMemoryManager = memoryManager;
    this.taskManager = taskManager;
    this.taskContext = taskContext;
    this.PARTITION_GROUP_PUSH_BUFFER_SIZE = partitionGroupPushBufferSize;
    this.partitionGroupManager = partitionGroupManager;
    this.writeMetrics = writeMetrics;
    this.inMemSorter = new ShuffleInMemorySorter(
      this, initialSize, conf.getBoolean("spark.shuffle.sort.useRadixSort", true));
    this.peakMemoryUsedBytes = getMemoryUsage();
    this.sortPushSpillSizeThreshold = sortPushSpillSizeThreshold;
    this.sortPushSpillRecordThreshold = sortPushSpillRecordThreshold;
  }

  private void mergeSpillToCss() {

    // This call performs the actual sort.
    final ShuffleInMemorySorter.ShuffleSorterIterator sortedRecords =
      inMemSorter.getSortedIterator();

    // If there are no sorted records, so we don't need to create an empty spill file.
    if (!sortedRecords.hasNext()) {
      return;
    }

    byte[] giantBuffer = null;
    final byte[] writeBuffer = new byte[PARTITION_GROUP_PUSH_BUFFER_SIZE];
    int writeBufferOffset = 0;
    long recordsWritten = 0L;

    List<Integer> offsets = new ArrayList<>();
    List<Integer> lengths = new ArrayList<>();
    List<Integer> partitionIds = new ArrayList<>();

    int currentPartition = -1;
    int currentGroup = -1;
    int currentPartitionStartOffset = 0;
    final int uaoSize = UnsafeAlignedOffset.getUaoSize();

    while (sortedRecords.hasNext()) {
      sortedRecords.loadNext();
      final int partition = sortedRecords.packedRecordPointer.getPartitionId();
      final int group = partitionGroupManager.groupId(partition);
      assert (partition >= currentPartition);
      if (partition != currentPartition) {
        // Record the start offset and partitionId of the current partition when the partition is switched
        switchPartition(
          writeBufferOffset,
          offsets,
          lengths,
          partitionIds,
          currentPartition,
          currentPartitionStartOffset
        );
        currentPartitionStartOffset = writeBufferOffset;
        currentPartition = partition;
      }

      if (group != currentGroup) {
        // Switch the group and flush out the data of the old group
        if (writeBufferOffset > 0) {
          switchPartition(
            writeBufferOffset,
            offsets,
            lengths,
            partitionIds,
            currentPartition,
            currentPartitionStartOffset
          );
          taskManager.safeDirectBatchPush(
            partitionIds.stream().mapToInt(x -> x).toArray(),
            writeBuffer,
            offsets.stream().mapToInt(x -> x).toArray(),
            lengths.stream().mapToInt(x -> x).toArray());
          resetMeta(offsets, lengths, partitionIds);
          currentPartitionStartOffset = 0;
          writeBufferOffset = 0;
        }
        currentGroup = group;
      }

      final long recordPointer = sortedRecords.packedRecordPointer.getRecordPointer();
      final Object recordPage = taskMemoryManager.getPage(recordPointer);
      final long recordOffsetInPage = taskMemoryManager.getOffsetInPage(recordPointer);
      int dataRemaining = UnsafeAlignedOffset.getSize(recordPage, recordOffsetInPage);
      long recordReadPosition = recordOffsetInPage + uaoSize; // skip over record length
      if (dataRemaining > PARTITION_GROUP_PUSH_BUFFER_SIZE) {
        // trigger direct push
        // no need to update writeBufferOffset
        // since we are using separate giantBuffer for directPush
        if (giantBuffer == null || giantBuffer.length < dataRemaining) {
          giantBuffer = new byte[dataRemaining];
        }
        Platform.copyMemory(recordPage, recordReadPosition,
          giantBuffer, Platform.BYTE_ARRAY_OFFSET, dataRemaining);
        // update to new direct push api
        taskManager.safeDirectBatchPush(
          new int[] {currentPartition},
          giantBuffer,
          new int[] {0},
          new int[] {dataRemaining});
      } else {
        if (PARTITION_GROUP_PUSH_BUFFER_SIZE - writeBufferOffset < dataRemaining) {
          switchPartition(
            writeBufferOffset,
            offsets,
            lengths,
            partitionIds,
            currentPartition,
            currentPartitionStartOffset
          );
          // writeBuffer no enough space for current record
          // trigger normal push and clear buffer
          taskManager.safeDirectBatchPush(
            partitionIds.stream().mapToInt(x -> x).toArray(),
            writeBuffer,
            offsets.stream().mapToInt(x -> x).toArray(),
            lengths.stream().mapToInt(x -> x).toArray());
          resetMeta(offsets, lengths, partitionIds);
          currentPartitionStartOffset = 0;
          writeBufferOffset = 0;
        }

        // just write into writeBuffer
        Platform.copyMemory(recordPage, recordReadPosition,
          writeBuffer, Platform.BYTE_ARRAY_OFFSET + writeBufferOffset, dataRemaining);
        writeBufferOffset += dataRemaining;
      }
      recordsWritten += 1;
    }

    if (writeBufferOffset > 0) {
      switchPartition(
        writeBufferOffset,
        offsets,
        lengths,
        partitionIds,
        currentPartition,
        currentPartitionStartOffset);
      taskManager.safeDirectBatchPush(
        partitionIds.stream().mapToInt(x -> x).toArray(),
        writeBuffer,
        offsets.stream().mapToInt(x -> x).toArray(),
        lengths.stream().mapToInt(x -> x).toArray());
      resetMeta(offsets, lengths, partitionIds);
      currentPartitionStartOffset = 0;
      writeBufferOffset = 0;
    }

    writeMetrics.incRecordsWritten(recordsWritten);
    spillTimes += 1;
  }

  private void resetMeta(List<Integer> offsets, List<Integer> lengths, List<Integer> partitionIds) {
    offsets.clear();
    lengths.clear();
    partitionIds.clear();
  }

  private void switchPartition(
      int writeBufferOffset,
      List<Integer> offsets,
      List<Integer> lengths,
      List<Integer> partitionIds,
      int currentPartition,
      int currentPartitionStartOffset) {
    if (writeBufferOffset > currentPartitionStartOffset &&
      (partitionIds.size() == 0 || partitionIds.get(partitionIds.size() - 1) != currentPartition)) {
      partitionIds.add(currentPartition);
      offsets.add(currentPartitionStartOffset);
      lengths.add(writeBufferOffset - currentPartitionStartOffset);
    }
  }

  /**
   * Sort and spill the current records in response to memory pressure.
   */
  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    if (trigger != this || inMemSorter == null || inMemSorter.numRecords() == 0) {
      return 0L;
    }

    logger.info("Thread {} spilling sort data of {} to css ({} {} so far)",
      Thread.currentThread().getId(),
      Utils.bytesToString(getMemoryUsage()),
      spillTimes,
      spillTimes > 1 ? " times" : " time");

    mergeSpillToCss();

    final long spillSize = freeMemory();
    inMemSorter.reset();
    return spillSize;
  }

  private long getMemoryUsage() {
    long totalPageSize = 0;
    for (MemoryBlock page : allocatedPages) {
      totalPageSize += page.size();
    }
    return ((inMemSorter == null) ? 0 : inMemSorter.getMemoryUsage()) + totalPageSize;
  }

  private void updatePeakMemoryUsed() {
    long mem = getMemoryUsage();
    if (mem > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = mem;
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  private long freeMemory() {
    updatePeakMemoryUsed();
    long memoryFreed = 0;
    for (MemoryBlock block : allocatedPages) {
      memoryFreed += block.size();
      freePage(block);
    }
    allocatedPages.clear();
    currentPage = null;
    pageCursor = 0;
    return memoryFreed;
  }

  /**
   * Force all memory and spill files to be deleted; called by shuffle error-handling code.
   */
  public void cleanupResources() {
    freeMemory();
    if (inMemSorter != null) {
      inMemSorter.free();
      inMemSorter = null;
    }
  }

  /**
   * Checks whether there is enough space to insert an additional record in to the sort pointer
   * array and grows the array if additional space is required. If the required space cannot be
   * obtained, then the in-memory data will be spilled to disk.
   */
  private void growPointerArrayIfNecessary() throws IOException {
    assert(inMemSorter != null);
    if (!inMemSorter.hasSpaceForAnotherRecord()) {
      long used = inMemSorter.getMemoryUsage();
      LongArray array;
      try {
        // could trigger spilling
        array = allocateArray(used / 8 * 2);
      } catch (TooLargePageException e) {
        // The pointer array is too big to fix in a single page, spill.
        spill();
        return;
      } catch (SparkOutOfMemoryError e) {
        // should have trigger spilling
        if (!inMemSorter.hasSpaceForAnotherRecord()) {
          logger.error("Unable to grow the pointer array");
          throw e;
        }
        return;
      }
      // check if spilling is triggered or not
      if (inMemSorter.hasSpaceForAnotherRecord()) {
        freeArray(array);
      } else {
        inMemSorter.expandPointerArray(array);
      }
    }
  }

  /**
   * Allocates more memory in order to insert an additional record. This will request additional
   * memory from the memory manager and spill if the requested memory can not be obtained.
   *
   * @param required the required space in the data page, in bytes, including space for storing
   *                      the record size. This must be less than or equal to the page size (records
   *                      that exceed the page size are handled via a different code path which uses
   *                      special overflow pages).
   */
  private void acquireNewPageIfNecessary(int required) {
    if (currentPage == null ||
      pageCursor + required > currentPage.getBaseOffset() + currentPage.size() ) {
      // TODO: try to find space in previous pages
      currentPage = allocatePage(required);
      pageCursor = currentPage.getBaseOffset();
      allocatedPages.add(currentPage);
    }
  }

  /**
   * Write a record to the shuffle sorter.
   */
  public void insertRecord(Object recordBase, long recordOffset, int length, int partitionId)
      throws IOException {

    // for tests
    assert(inMemSorter != null);

    // record based spill
    if (inMemSorter.numRecords() >= sortPushSpillRecordThreshold) {
      logger.info("Spilling data because number of spilledRecords crossed the threshold " +
        sortPushSpillRecordThreshold);
      spill();
    }

    // in memory size based spill
    if (getMemoryUsage() >= sortPushSpillSizeThreshold) {
      logger.info("Spilling data because spilledSize crossed the threshold " +
        Utils.bytesToString(sortPushSpillSizeThreshold));
      spill();
    }

    growPointerArrayIfNecessary();
    final int uaoSize = UnsafeAlignedOffset.getUaoSize();
    // Need 4 or 8 bytes to store the record length.
    final int required = length + uaoSize;
    acquireNewPageIfNecessary(required);

    assert(currentPage != null);
    final Object base = currentPage.getBaseObject();
    final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);
    UnsafeAlignedOffset.putSize(base, pageCursor, length);
    pageCursor += uaoSize;
    Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);
    pageCursor += length;
    inMemSorter.insertRecord(recordAddress, partitionId);
  }

  public void close() throws IOException {
    if (inMemSorter != null) {
      spill();
      freeMemory();
      inMemSorter.free();
      inMemSorter = null;
    }
  }

}
