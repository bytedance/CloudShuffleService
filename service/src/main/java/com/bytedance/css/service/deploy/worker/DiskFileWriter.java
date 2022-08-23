/*
 * Copyright 2022 Bytedance Inc.
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

package com.bytedance.css.service.deploy.worker;

import com.bytedance.css.common.exception.AlreadyClosedException;
import com.bytedance.css.common.exception.EpochShouldRotateException;
import com.bytedance.css.common.protocol.ShuffleMode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class DiskFileWriter implements FileWriter {

  private static final Logger logger = LoggerFactory.getLogger(DiskFileWriter.class);

  private static final long WAIT_INTERVAL_MS = 20;
  private static final int COMPOSITE_BYTE_BUF_MAX_NUM = 128;
  private final FileNotifier notifier = new FileNotifier();

  private final File file;
  private final FileChannel channel;
  private volatile boolean closed;
  private volatile boolean rotated;

  private final AtomicInteger numPendingWrites = new AtomicInteger();
  private final ArrayList<Long> chunkOffsets = new ArrayList<>();
  private long nextBoundary;
  private long bytesFlushed = 0L;
  private long bytesSubmitted = 0L;

  private final FileFlusher flusher;
  private volatile boolean testSubmitFlushTaskException = false;
  private CompositeByteBuf compositeByteBuf = null;

  private final long chunkSize;
  private final long timeoutMs;
  private final long flushBufferSize;
  private final long epochRotateThreshold;

  public DiskFileWriter(
      File file,
      FileFlusher flusher,
      long chunkSize,
      long timeoutMs,
      long flushBufferSize,
      long epochRotateThreshold) throws IOException {
    this.file = file;
    this.channel = new FileOutputStream(file).getChannel();
    this.flusher = flusher;
    this.chunkSize = chunkSize;
    this.timeoutMs = timeoutMs;
    this.flushBufferSize = flushBufferSize;
    this.epochRotateThreshold = epochRotateThreshold;
    this.nextBoundary = chunkSize;
    this.chunkOffsets.add(0L);
    FileWriterMetrics.instance().diskOpenedFileNum.inc();
  }

  private void submitFlushTask(FlushTask task) throws IOException {
    if (testSubmitFlushTaskException || !flusher.submitTask(task, timeoutMs)) {
      String msg = String.format("DiskFileWriter submit flush task timeout with file %s.", file.getAbsolutePath());
      IOException e = new IOException(msg);
      notifier.setException(e);
      throw e;
    }
  }

  private void flushBuffer(ByteBuf data, boolean finalFlush) throws IOException {
    ByteBuf toBeFlushed = data == null ? compositeByteBuf : data;
    notifier.getNumPendingFlushes().incrementAndGet();
    int numBytes = toBeFlushed.readableBytes();
    FlushTask task = new DiskFileFlushTask(toBeFlushed, notifier, channel);
    submitFlushTask(task);
    bytesFlushed += numBytes;
    updateChunkOffsets(finalFlush);
  }

  private void updateChunkOffsets(boolean forceSet) {
    if (bytesFlushed >= nextBoundary || forceSet) {
      chunkOffsets.add(bytesFlushed);
      nextBoundary = bytesFlushed + chunkSize;
    }
  }

  private void waitOnNoPending(AtomicInteger counter) throws IOException {
    long waitTime = timeoutMs;
    while (counter.get() > 0 && waitTime > 0) {
      try {
        notifier.checkException();
        TimeUnit.MILLISECONDS.sleep(WAIT_INTERVAL_MS);
      } catch (InterruptedException e) {
        IOException ioe = new IOException(e);
        notifier.setException(ioe);
        throw ioe;
      }
      waitTime -= WAIT_INTERVAL_MS;
    }
    if (counter.get() > 0) {
      String msg = String.format("DiskFileWriter wait pending actions timeout with %s .", file.getAbsolutePath());
      IOException ioe = new IOException(msg);
      notifier.setException(ioe);
      throw ioe;
    }
    notifier.checkException();
  }

  @Override
  public ArrayList<Long> getChunkOffsets() {
    return chunkOffsets;
  }

  @Override
  public void write(ByteBuf data, int mapperId, boolean ignoreRotate) throws IOException {
    if (!ignoreRotate & rotated) {
      // normally we don't hit this code branch, since we already call FileWriter.shouldRotate
      // before write data. Just to in case
      String msg = String.format("DiskFileWriter with %s rotated.", file.getAbsolutePath());
      logger.warn(msg);
      throw new EpochShouldRotateException(msg);
    } else if (closed) {
      String msg = String.format("DiskFileWriter write with %s closed.", file.getAbsolutePath());
      logger.warn(msg);
      throw new AlreadyClosedException(msg);
    }

    synchronized (this) {
      notifier.checkException();

      if (compositeByteBuf == null) {
        compositeByteBuf = Unpooled.compositeBuffer(COMPOSITE_BYTE_BUF_MAX_NUM);
      }
      final int numBytes = data.readableBytes();
      data.retain();

      try {
        if (flushBufferSize < numBytes) {
          String msg = String.format("DiskFileWriter Flush giant batch with %s numBytes %s.",
            file.getAbsolutePath(), numBytes);
          logger.debug(msg);
          flushBuffer(data, false);
        } else {
          // buffer not enough for data batch, trigger flush buffer to release memory
          if (compositeByteBuf.readableBytes() + numBytes >= flushBufferSize) {
            // buffer full, submit old compositeByteBuf to taskQueue, and re-create another one.
            flushBuffer(null, false);
            compositeByteBuf = Unpooled.compositeBuffer(COMPOSITE_BYTE_BUF_MAX_NUM);
          }

          compositeByteBuf.addComponent(true, data);
        }
      } catch (IOException e) {
        // here we only need to release data. compositeByteBuf will release by close & destroy.
        ReferenceCountUtil.safeRelease(data);
        throw e;
      }

      bytesSubmitted += numBytes;
      if (bytesSubmitted > epochRotateThreshold) {
        rotated = true;
        flushBuffer(null, !ignoreRotate);
        compositeByteBuf = Unpooled.compositeBuffer(COMPOSITE_BYTE_BUF_MAX_NUM);
      }

      numPendingWrites.decrementAndGet();
    }
  }

  @Override
  public long close() throws IOException {
    if (closed) {
      // if reallocation happens, the old partition will be closed in advanced
      // to save StageEnd time.
      checkException();
      String msg = String.format("DiskFileWriter with %s closed.", file.getAbsolutePath());
      logger.warn(msg);
      return bytesFlushed;
    }

    try {
      waitOnNoPending(numPendingWrites);
      closed = true;

      synchronized (this) {
        if (compositeByteBuf != null && compositeByteBuf.readableBytes() > 0) {
          flushBuffer(null, true);
          compositeByteBuf = null;
        }
        if (chunkOffsets.get(chunkOffsets.size() - 1) != bytesFlushed) {
          updateChunkOffsets(true);
        }
      }

      waitOnNoPending(notifier.getNumPendingFlushes());
    } finally {
      if (compositeByteBuf != null) {
        ReferenceCountUtil.safeRelease(compositeByteBuf);
        compositeByteBuf = null;
      }
      channel.close();
      FileWriterMetrics.instance().diskOpenedFileNum.dec();
    }

    FileWriterMetrics.instance().diskRealFlushThroughput.mark(bytesFlushed);
    return bytesFlushed;
  }

  @Override
  public void destroy() {
    if (!closed) {
      closed = true;
      String msg = String.format("DiskFileWriter destroyed with path %s.", file.getAbsolutePath());
      logger.warn(msg);
      try {
        channel.close();
        FileWriterMetrics.instance().diskOpenedFileNum.dec();
      } catch (IOException e) {
        String error = String.format("DiskFileWriter close channel failed with %s", file.getAbsolutePath());
        logger.warn(error, e);
      } finally {
        if (compositeByteBuf != null) {
          ReferenceCountUtil.safeRelease(compositeByteBuf);
          compositeByteBuf = null;
        }
      }
    }
    file.delete();
  }

  @Override
  public void setException() {
    notifier.setException(new IOException("ForTestOnly"));
  }

  @Override
  public void setSubmitQueueFullException(boolean exception) {
    testSubmitFlushTaskException = exception;
  }

  @Override
  public CompositeByteBuf getCompositeByteBuf() {
    return compositeByteBuf;
  }

  @Override
  public void checkException() throws IOException {
    notifier.checkException();
  }

  @Override
  public void incrementPendingWrites() {
    numPendingWrites.incrementAndGet();
  }

  @Override
  public void decrementPendingWrites() {
    numPendingWrites.decrementAndGet();
  }

  @Override
  public ShuffleMode getShuffleMode() {
    return ShuffleMode.DISK;
  }

  @Override
  public String getFilePath() {
    return file.getAbsolutePath();
  }

  @Override
  public long getFileLength() {
    return file.length();
  }

  @Override
  public File getFile() {
    return file;
  }

  @Override
  public boolean shouldRotate() {
    return rotated;
  }
}
