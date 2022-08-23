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

package com.bytedance.css.client.stream;

import com.bytedance.css.client.compress.Compressor;
import com.bytedance.css.client.compress.CompressorFactory;
import com.bytedance.css.client.compress.CssCompressorFactory;
import com.bytedance.css.client.stream.disk.CssRemoteDiskEpochReader;
import com.bytedance.css.common.CssConf;
import com.bytedance.css.common.protocol.CommittedPartitionInfo;
import com.bytedance.css.common.protocol.FailedPartitionInfoBatch;
import com.bytedance.css.common.protocol.ShuffleMode;
import com.bytedance.css.common.unsafe.Platform;
import com.bytedance.css.common.util.Utils;
import com.bytedance.css.network.TransportContext;
import com.bytedance.css.network.client.RpcResponseCallback;
import com.bytedance.css.network.client.TransportClient;
import com.bytedance.css.network.client.TransportClientFactory;
import com.bytedance.css.network.protocol.shuffle.BlockTransferMessage;
import com.bytedance.css.network.protocol.shuffle.OpenStream;
import com.bytedance.css.network.protocol.shuffle.StreamHandle;
import com.bytedance.css.network.server.*;
import com.bytedance.css.network.util.TransportConf;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CssDiskInputStreamSuite {

  private HashMap<String, CssFileInfo> map = new HashMap<>();
  private TransportContext transportContext = null;
  private TransportServer transportServer = null;

  class MockRpcHandler extends RpcHandler {

    private final TransportConf conf;
    private final OneForOneStreamManager streamManager;

    MockRpcHandler(TransportConf conf) {
      this.conf = conf;
      streamManager = new OneForOneStreamManager();
    }

    @Override
    public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
      BlockTransferMessage msg = BlockTransferMessage.Decoder.fromByteBuffer(message);
      OpenStream openStream = (OpenStream) msg;
      String shuffleKey = openStream.shuffleKey;
      String fileName = openStream.filePath;
      int chunkIndex = openStream.initChunkIndex;
      CssFileInfo fileInfo = map.get(shuffleKey + "-" + fileName);

      try {
        CssManagedBufferIterator iterator = new CssManagedBufferIterator(fileInfo, conf);
        iterator.setInitIndex(chunkIndex);
        long streamId = streamManager.registerStream(
          client.getClientId(), iterator, client.getChannel());
        streamManager.setStreamStateCurIndex(streamId, chunkIndex);

        StreamHandle streamHandle = new StreamHandle(streamId, fileInfo.numChunks);
        callback.onSuccess(streamHandle.toByteBuffer());
      } catch (IOException e) {
        callback.onFailure(new Exception("Chunk offsets meta exception ", e));
      }
    }

    @Override
    public StreamManager getStreamManager() {
      return streamManager;
    }
  }

  @Before
  public void startServer() throws Exception {
    map.clear();
    TransportConf transportConf = Utils.fromCssConf(new CssConf(), "TEST", 0);
    MockRpcHandler rpcHandler = new MockRpcHandler(transportConf);
    transportContext = new TransportContext(transportConf, rpcHandler, false);
    transportServer = transportContext.createServer(12345, new ArrayList<>());
  }

  @After
  public void stopServer() throws Exception {
    if (transportServer != null) {
      transportServer.close();
    }
    if (transportContext != null) {
      transportContext.close();
    }
  }

  private byte[] addHeaderAndCompress(
      CssConf cssConf,
      int mapperId,
      int mapperAttemptId,
      int batchId,
      byte[] originalBytes) {
    CompressorFactory factory = new CssCompressorFactory(cssConf);
    final Compressor compressor = factory.getCompressor();
    compressor.compress(originalBytes, 0, originalBytes.length);

    final int compressedTotalSize = compressor.getCompressedTotalSize();
    final int BATCH_HEADER_SIZE = 4 * 4;
    final byte[] body = new byte[BATCH_HEADER_SIZE + compressedTotalSize];
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET, mapperId);
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 4, mapperAttemptId);
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 8, batchId);
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 12, compressedTotalSize);
    System.arraycopy(compressor.getCompressedBuffer(), 0, body, BATCH_HEADER_SIZE, compressedTotalSize);

    return body;
  }


  @Test
  public void testCssDiskInputStream() throws Exception {
    CssConf cssConf = new CssConf();
    String[] localChunkEnables = new String[]{"true", "false"};
    for (String localChunkEnable : localChunkEnables) {
      cssConf.set("css.local.chunk.fetch.enabled", localChunkEnable);
      cssConf.set("css.chunk.fetch.retry.wait.times", "5ms");

      String[] compressTypes = new String[]{"lz4", "zstd"};
      for (String type : compressTypes) {
        cssConf.set("css.compression.codec", type);

        String shuffleKey = "DontTouchMe";
        TransportConf transportConf = Utils.fromCssConf(cssConf, "TEST", 0);
        TransportContext transportContext =
          new TransportContext(transportConf, new NoOpRpcHandler(), false);
        TransportClientFactory clientFactory = transportContext.createClientFactory(new ArrayList<>());

        for (int i = 0; i < 100; i++) {
          String master = "SHUFFLE-FILE-" + i + "MASTER";
          String slave = "SHUFFLE-FILE-" + i + "SLAVE";

          File file = File.createTempFile(type + "-ABC", "OUT.M");
          file.deleteOnExit();
          ArrayList<Long> chunkOffsets = new ArrayList<>();
          chunkOffsets.add(0L);
          long totalLength = 0L;
          long deDupLength = 0L;
          FileOutputStream outputStream = new FileOutputStream(file);

          // remain all real data string.
          List<String> originContentList = new ArrayList<>();
          // add random data for target mapper task.
          for (int j = 0; j < 1000; j++) {
            String content = RandomStringUtils.randomAlphanumeric(1024, 2048);
            originContentList.add(content);
            byte[] bytes = addHeaderAndCompress(cssConf, 0, 0, j, content.getBytes(StandardCharsets.UTF_8));
            outputStream.write(bytes);
            totalLength += bytes.length;
            deDupLength += content.length();
            chunkOffsets.add(totalLength);

            // add duplicate batch data with same mapper task.
            if (j % 80 == 0) {
              outputStream.write(bytes);
              totalLength += bytes.length;
              chunkOffsets.add(totalLength);
            }
          }

          List<FailedPartitionInfoBatch> failedPartitionInfoBatches = new ArrayList<>();
          // add failed batch data for current partition file.
          for (int j = 1500; j < 1520; j++) {
            String content = RandomStringUtils.randomAlphanumeric(1024, 2048);
            byte[] bytes = addHeaderAndCompress(cssConf, 0, 0, j, content.getBytes(StandardCharsets.UTF_8));
            outputStream.write(bytes);
            totalLength += bytes.length;
            chunkOffsets.add(totalLength);
            failedPartitionInfoBatches.add(new FailedPartitionInfoBatch(0, 0, 0, 0, j));
          }

          // add duplicate data with other attemptId
          for (int k = 0; k < 100; k++) {
            String content = RandomStringUtils.randomAlphanumeric(1024, 2048);
            byte[] bytes = addHeaderAndCompress(cssConf, 0, 1, k, content.getBytes(StandardCharsets.UTF_8));
            outputStream.write(bytes);
            totalLength += bytes.length;
            chunkOffsets.add(totalLength);
          }
          outputStream.close();

          // choose one replica available
          CssFileInfo fileInfo = new CssFileInfo(file, chunkOffsets, file.length());
          String fileKey = i % 2 == 0 ? master : slave;
          map.put(String.format("%s-%s", shuffleKey, fileKey), fileInfo);

          CommittedPartitionInfo[] partitions = new CommittedPartitionInfo[2];
          if (i % 2 == 0) {
            partitions[0] = new CommittedPartitionInfo(0, 0, Utils.localHostName(), 12345,
              ShuffleMode.DISK, fileKey, file.length());

            // invalid one
            partitions[1] = new CommittedPartitionInfo(0, 0, Utils.localHostName(), 54321,
              ShuffleMode.DISK, "/NeverMind", 100000L);
          } else {
            // invalid one
            partitions[0] = new CommittedPartitionInfo(0, 0, Utils.localHostName(), 54321,
              ShuffleMode.DISK, "/NeverMind", 100000L);

            partitions[1] = new CommittedPartitionInfo(0, 0, Utils.localHostName(), 12345,
              ShuffleMode.DISK, fileKey, file.length());
          }

          Set<String> failedBatchBlacklist = failedPartitionInfoBatches.stream().map(x ->
              String.format("%s-%s-%s-%s-%s",
                x.getReducerId(), x.getEpochId(), x.getMapId(), x.getAttemptId(), x.getBatchId()))
            .collect(Collectors.toSet());

          CssInputStream inputStream = CssInputStream.create(cssConf,
            clientFactory, shuffleKey, partitions, new int[]{0}, failedBatchBlacklist, 0, 1);

          long resultCount = 0L;
          int contentCount = 0;
          for (String originContent : originContentList) {
            byte[] bytes = new byte[originContent.length()];
            resultCount += inputStream.read(bytes, 0, bytes.length);
            contentCount++;
            assertEquals(originContent, new String(bytes, StandardCharsets.UTF_8));
          }
          assertEquals(inputStream.read(), -1); // inputStream already ended.
          assertEquals(originContentList.size(), contentCount);
          assertEquals(resultCount, deDupLength);

          inputStream.close();
        }

        clientFactory.close();
        transportContext.close();
      }
    }
  }

  @Test
  public void testCssDiskEpochReader() throws Exception {
    CssConf cssConf = new CssConf();
    cssConf.set("css.local.chunk.fetch.enabled", "false");
    cssConf.set("css.chunk.fetch.retry.wait.times", "5ms");

    String shuffleKey = "DontTouchMe";
    TransportConf transportConf = Utils.fromCssConf(cssConf, "TEST", 0);
    TransportContext transportContext =
      new TransportContext(transportConf, new NoOpRpcHandler(), false);
    TransportClientFactory clientFactory = transportContext.createClientFactory(new ArrayList<>());

    for (int i = 0; i < 100; i++) {
      String master = "SHUFFLE-FILE-" + i + "MASTER";
      String slave = "SHUFFLE-FILE-" + i + "SLAVE";

      File file = File.createTempFile("ABC", "OUT.M");
      file.deleteOnExit();
      ArrayList<Long> chunkOffsets = new ArrayList<>();
      chunkOffsets.add(0L);
      long totalLength = 0L;
      FileOutputStream outputStream = new FileOutputStream(file);
      HashSet<String> set = new HashSet<>();
      for (int j = 0; j < 1000; j++) {
        String content = RandomStringUtils.randomAlphanumeric(1024, 2048);
        set.add(content);
        byte[] bytes = new byte[4 + content.length()];
        Platform.putInt(bytes, Platform.BYTE_ARRAY_OFFSET, content.length());
        System.arraycopy(content.getBytes(StandardCharsets.UTF_8), 0, bytes, 4, content.length());
        outputStream.write(bytes);
        totalLength += bytes.length;
        chunkOffsets.add(totalLength);
      }
      outputStream.close();

      CssFileInfo fileInfo = new CssFileInfo(file, chunkOffsets, file.length());
      String key = i % 2 == 0 ? master : slave;
      map.put(String.format("%s-%s", shuffleKey, key), fileInfo);

      CommittedPartitionInfo[] partitions = new CommittedPartitionInfo[2];
      if (i % 2 == 0) {
        partitions[0] = new CommittedPartitionInfo(0, 0, Utils.localHostName(), 12345,
          ShuffleMode.DISK, key, file.length());

        // invalid one
        partitions[1] = new CommittedPartitionInfo(0, 0, Utils.localHostName(), 54321,
          ShuffleMode.DISK, "/NeverMind", 100000L);
      } else {
        // invalid one
        partitions[0] = new CommittedPartitionInfo(0, 0, Utils.localHostName(), 54321,
          ShuffleMode.DISK, "/NeverMind", 100000L);

        partitions[1] = new CommittedPartitionInfo(0, 0, Utils.localHostName(), 12345,
          ShuffleMode.DISK, key, file.length());
      }

      CssRemoteDiskEpochReader reader = new CssRemoteDiskEpochReader(cssConf, clientFactory, shuffleKey, partitions);

      byte[] sizeBuf = new byte[4];
      while (reader.hasNext()) {
        ByteBuf buffer = reader.next();
        buffer.readBytes(sizeBuf);
        int tmpLength = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET);
        byte[] tmpBuffer = new byte[tmpLength];
        buffer.readBytes(tmpBuffer);
        String content = new String(tmpBuffer);
        assertTrue(set.contains(content));
        set.remove(content);
      }
      assertTrue(set.isEmpty());
      reader.close();
    }

    clientFactory.close();
    transportContext.close();
  }
}
