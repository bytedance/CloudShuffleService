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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class HdfsFileWriter implements FileWriter {

  private static final Logger logger = LoggerFactory.getLogger(HdfsFileWriter.class);
  private static final long WAIT_INTERVAL_MS = 20;
  private static final int COMPOSITE_BYTE_BUF_MAX_NUM = 32;
  private final FileNotifier notifier = new FileNotifier();

  private final FileSystem fs;
  private final Path path;
  private final FSDataOutputStream outputStream;
  private volatile boolean closed;
  private volatile boolean rotated;

  private final AtomicInteger numPendingWrites = new AtomicInteger();
  private long bytesFlushed = 0L;
  private long bytesSubmitted = 0L;

  private final FileFlusher flusher;
  private volatile boolean testSubmitFlushTaskException = false;
  private CompositeByteBuf compositeByteBuf = null;

  private final long timeoutMs;
  private final long flushBufferSize;
  private final long epochRotateThreshold;

  public HdfsFileWriter(
      FileSystem fs,
      Path path,
      FSDataOutputStream outputStream,
      FileFlusher flusher,
      long timeoutMs,
      long flushBufferSize,
      long epochRotateThreshold) {
    this.fs = fs;
    this.path = path;
    this.outputStream = outputStream;
    this.flusher = flusher;
    this.timeoutMs = timeoutMs;
    this.flushBufferSize = flushBufferSize;
    this.epochRotateThreshold = epochRotateThreshold;
    FileWriterMetrics.instance().hdfsOpenedFileNum.inc();
  }

  private void submitFlushTask(FlushTask task) throws IOException {
    if (testSubmitFlushTaskException || !flusher.submitTask(task, timeoutMs)) {
      String msg = String.format("HdfsFileWriter submit flush task timeout with file %s.", path);
      IOException e = new IOException(msg);
      notifier.setException(e);
      throw e;
    }
  }

  private void flushBuffer(ByteBuf data) throws IOException {
    ByteBuf toBeFlushed = data == null ? compositeByteBuf : data;
    notifier.getNumPendingFlushes().incrementAndGet();
    int numBytes = toBeFlushed.readableBytes();
    FlushTask task = new HdfsFileFlushTask(toBeFlushed, notifier, outputStream);
    submitFlushTask(task);
    bytesFlushed += numBytes;
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
      String msg = String.format("HdfsFileWriter wait pending actions timeout with %s .", path);
      IOException ioe = new IOException(msg);
      notifier.setException(ioe);
      throw ioe;
    }
    notifier.checkException();
  }

  @Override
  public ArrayList<Long> getChunkOffsets() {
    throw new UnsupportedOperationException("HdfsFileWriter does not support chunk split.");
  }

  @Override
  public void write(ByteBuf data, int mapperId, boolean ignoreRotate) throws IOException {
    if (!ignoreRotate & rotated) {
      // normally we don't hit this code branch, since we already call FileWriter.shouldRotate
      // before write data. Just to in case
      String msg = String.format("HdfsFileWriter with %s rotated.", path);
      logger.warn(msg);
      throw new EpochShouldRotateException(msg);
    } else if (closed) {
      String msg = String.format("HdfsFileWriter write with %s closed.", path);
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
          String msg = String.format("HdfsFileWriter Flush giant batch with %s numBytes %s.", path, numBytes);
          logger.debug(msg);
          flushBuffer(data);
        } else {
          // buffer not enough for data batch, trigger flush buffer to release memory
          if (compositeByteBuf.readableBytes() + numBytes >= flushBufferSize) {
            flushBuffer(null);
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
        flushBuffer(null);
        compositeByteBuf = Unpooled.compositeBuffer(COMPOSITE_BYTE_BUF_MAX_NUM);
        String msg = String.format("HdfsFileWriter for %s enter rotate state.", path);
        logger.warn(msg);
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
      String msg = String.format("HdfsFileWriter with %s closed.", path);
      logger.warn(msg);
      return bytesFlushed;
    }

    try {
      waitOnNoPending(numPendingWrites);
      closed = true;

      synchronized (this) {
        if (compositeByteBuf != null && compositeByteBuf.readableBytes() > 0) {
          flushBuffer(null);
          compositeByteBuf = null;
        }
      }

      waitOnNoPending(notifier.getNumPendingFlushes());
    } finally {
      if (compositeByteBuf != null) {
        ReferenceCountUtil.safeRelease(compositeByteBuf);
        compositeByteBuf = null;
      }
      outputStream.close();
      FileWriterMetrics.instance().hdfsOpenedFileNum.dec();
    }

    return bytesFlushed;
  }

  @Override
  public void destroy() {
    if (!closed) {
      closed = true;
      String msg = String.format("HdfsFileWriter destroyed with path %s.", path);
      logger.warn(msg);
      try {
        outputStream.close();
        FileWriterMetrics.instance().hdfsOpenedFileNum.dec();
      } catch (IOException e) {
        String error = String.format("HdfsFileWriter close channel failed with %s", path);
        logger.warn(error, e);
      } finally {
        if (compositeByteBuf != null) {
          ReferenceCountUtil.safeRelease(compositeByteBuf);
          compositeByteBuf = null;
        }
      }
    }
    try {
      fs.delete(path, true);
    } catch (IOException e) {
      logger.error("HdfsFileWriter failed to delete hdfs file {}.", path);
    }
  }

  @Override
  public void setException() {
    notifier.setException(new IOException("ForTestOnly"));
  }

  @Override
  public void setSubmitQueueFullException(boolean exception) {
    testSubmitFlushTaskException = true;
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
    return ShuffleMode.HDFS;
  }

  @Override
  public String getFilePath() {
    return path.toUri().toString();
  }

  @Override
  public long getFileLength() {
    throw new UnsupportedOperationException("HdfsFileWriter does not support getFileLength api.");
  }

  @Override
  public File getFile() {
    throw new UnsupportedOperationException("HdfsFileWriter does not support getFile api.");
  }

  @Override
  public boolean shouldRotate() {
    return rotated;
  }
}
