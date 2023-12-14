/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc.impl;

import org.apache.hadoop.io.BytesWritable;
import org.apache.orc.CompressionCodec;
import org.apache.orc.PhysicalWriter;
import org.apache.orc.impl.writer.StreamOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.util.function.Consumer;

/**
 * The output stream for writing to ORC files.
 * 用于写入ORC文件的输出流。
 * It handles both compression and encryption.
 * 它同时处理压缩和加密。
 */
public class OutStream extends PositionedOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(OutStream.class);

  // This logger will log the local keys to be printed to the logs at debug.
  // Be *extremely* careful turning it on.
  static final Logger KEY_LOGGER = LoggerFactory.getLogger("org.apache.orc.keys");

  // 三个字节的首部，只有配置压缩的时候才生效
  public static final int HEADER_SIZE = 3;
  // 流名称
  private final Object name;
  // 物理写出工具
  private final PhysicalWriter.OutputReceiver receiver;

  /**
   * 存储已序列化但尚未压缩的未压缩字节。
   * Stores the uncompressed bytes that have been serialized, but not compressed yet.
   * 当填充时，我们压缩整个缓冲区。
   * When this fills, we compress the entire buffer.
   */
  private ByteBuffer current = null;

  /**
   * 存储压缩字节，直到我们有一个完整的缓冲区，然后将它们输出到接收器。
   * Stores the compressed bytes until we have a full buffer and then outputs them to the receiver.
   * 如果没有进行压缩，则此（和溢出）将始终为空，当前缓冲区将直接发送到接收器。
   * If no compression is being done, this (and overflow)  will always be null and the current buffer will be sent directly to the receiver.
   */
  private ByteBuffer compressed = null;

  /**
   * Since the compressed buffer may start with contents from previous compression blocks,
   * 由于压缩缓冲器可以从先前压缩块的内容开始，
   * we allocate an overflow buffer so that the output of the codec can be split between the two buffers.
   * 我们分配一个溢出缓冲区，以便编解码器的输出可以在两个缓冲区之间分割。
   * After the compressed buffer is sent to the receiver, the overflow buffer becomes the new compressed buffer.
   * 压缩缓冲区发送到接收器后，溢出缓冲区成为新的压缩缓冲区。
   */
  private ByteBuffer overflow = null;

  private final int bufferSize;                     // 数据缓冲区大小  --  orc.compress.size
  private final CompressionCodec codec;             // 流压缩类型
  private final CompressionCodec.Options options;   // 压缩选项

  private long compressedBytes = 0;                 // 当前压缩数据长度
  private long uncompressedBytes = 0;               // 当前 current 中的数据量
  // 加密
  private final Cipher cipher;
  private final Key key;
  private final byte[] iv;

  public OutStream(Object name,
                   StreamOptions options,
                   PhysicalWriter.OutputReceiver receiver) {

    this.name = name;                             // StreamName
    this.bufferSize = options.getBufferSize();    //
    this.codec = options.getCodec();              // 压缩算法
    this.options = options.getCodecOptions();     // 压缩选项
    this.receiver = receiver;                     // 数据写出器

    if (options.isEncrypted()) {   // 开启列加密
      this.cipher = options.getAlgorithm().createCipher();
      this.key = options.getKey();
      this.iv = options.getIv();
      resetState();
    } else {
      this.cipher = null;
      this.key = null;
      this.iv = null;
    }
    LOG.debug("Stream {} written to with {}", name, options);
    logKeyAndIv(name, key, iv);
  }

  static void logKeyAndIv(Object name, Key key, byte[] iv) {
    if (iv != null && KEY_LOGGER.isDebugEnabled()) {
      KEY_LOGGER.debug("Stream: {} Key: {} IV: {}", name,
          new BytesWritable(key.getEncoded()), new BytesWritable(iv));
    }
  }

  /**
   * Change the current Initialization Vector (IV) for the encryption.
   * @param modifier a function to modify the IV in place
   */
  @Override
  public void changeIv(Consumer<byte[]> modifier) {
    if (iv != null) {
      modifier.accept(iv);
      resetState();
      logKeyAndIv(name, key, iv);
    }
  }

  /**
   * 更改IV后重置密码。
   * Reset the cipher after changing the IV.
   */
  private void resetState() {
    try {
      cipher.init(Cipher.ENCRYPT_MODE, key, new IvParameterSpec(iv));
    } catch (InvalidKeyException e) {
      throw new IllegalStateException("ORC bad encryption key for " + this, e);
    } catch (InvalidAlgorithmParameterException e) {
      throw new IllegalStateException("ORC bad encryption parameter for " + this, e);
    }
  }

  /**
   * When a buffer is done, we send it to the receiver to store.
   * 缓冲区完成后，我们将其发送到接收器进行存储。
   * If we are encrypting, encrypt the buffer before we pass it on.
   * 如果要加密，则在传递缓冲区之前对其进行加密。
   * @param buffer the buffer to store
   */
  void outputBuffer(ByteBuffer buffer) throws IOException {
    if (cipher != null) {
      ByteBuffer output = buffer.duplicate();
      int len = buffer.remaining();
      try {
        int encrypted = cipher.update(buffer, output);
        output.flip();
        receiver.output(output);
        if (encrypted != len) {
          throw new IllegalArgumentException("Encryption of incomplete buffer "
              + len + " -> " + encrypted + " in " + this);
        }
      } catch (ShortBufferException e) {
        throw new IOException("Short buffer in encryption in " + this, e);
      }
    } else {
      // 将buffer刷给receiver
      receiver.output(buffer);
    }
  }

  /**
   * Ensure that the cipher didn't save any data.
   * The next call should be to changeIv to restart the encryption on a new IV.
   */
  void finishEncryption() {
    try {
      byte[] finalBytes = cipher.doFinal();
      if (finalBytes != null && finalBytes.length != 0) {
        throw new IllegalStateException("We shouldn't have remaining bytes " + this);
      }
    } catch (IllegalBlockSizeException e) {
      throw new IllegalArgumentException("Bad block size", e);
    } catch (BadPaddingException e) {
      throw new IllegalArgumentException("Bad padding", e);
    }
  }

  /**
   * Write the length of the compressed bytes.
   * 写入压缩字节的长度。
   * Life is much easier if the header is constant length, so just use 3 bytes.
   * 如果报头的长度是恒定的，那幺使用3字节就更容易了。
   * Considering most of the codecs want between 32k (snappy) and 256k (lzo, zlib),
   * 考虑到大多数编解码器需要在32k（snappy）和256k（lzo、zlib）之间，
   * 3 bytes should be plenty.
   * 3字节应该足够了。
   * We also use the low bit for whether it is the original or compressed bytes.
   * 我们还使用低位来确定它是原始字节还是压缩字节。
   * @param buffer the buffer to write the header to
   * @param position the position in the buffer to  at
   * @param val the size in the file
   * @param original is it uncompressed
   */
  private static void writeHeader(ByteBuffer buffer,   // 写出流
                                  int position,        // 写入位置
                                  int val,             // 写入值
                                  boolean original) {  // 是否是压缩
    // 写入三个字节
    buffer.put(position, (byte) ((val << 1) + (original ? 1 : 0)));  // 低位是0表示压缩，低位是1表示未压缩
    buffer.put(position + 1, (byte) (val >> 7));
    buffer.put(position + 2, (byte) (val >> 15));
  }

  /***
   * 初始化一个新的ByteBuffer，给current
   */
  private void getNewInputBuffer() {
    if (codec == null) {
      // 如果没有压缩，则只申请buffer_size大小
      current = ByteBuffer.allocate(bufferSize);
    } else {
      // 对于压缩数据需要多加一个首部写入首部压缩信息跟长度
      current = ByteBuffer.allocate(bufferSize + HEADER_SIZE);
      // 写入一个首部信息， 指明数据长度跟压缩标志
      writeHeader(current, 0, bufferSize, true);
      current.position(HEADER_SIZE);
    }
  }

  /**
   * 校验buffer_size 的大小
   * Throws exception if the bufferSize argument equals or exceeds 2^(3*8 - 1).
   * See {@link OutStream#writeHeader(ByteBuffer, int, int, boolean)}.
   * The bufferSize needs to be expressible in 3 bytes, and uses the least significant byte
   * to indicate original/compressed bytes.
   * @param bufferSize The ORC compression buffer size being checked.
   * @throws IllegalArgumentException If bufferSize value exceeds threshold.
   */
  public static void assertBufferSizeValid(int bufferSize) throws IllegalArgumentException {
    if (bufferSize >= (1 << 23)) {
      throw new IllegalArgumentException("Illegal value of ORC compression buffer size: " + bufferSize);
    }
  }

  /**
   * Allocate a new output buffer if we are compressing.
   * 获取一个新的用于压缩的ByteBuffer,提供给 compressd 或者 overflow
   */
  private ByteBuffer getNewOutputBuffer() {
    return ByteBuffer.allocate(bufferSize + HEADER_SIZE);
  }

  // 写切换读
  private void flip() {
    current.limit(current.position());                  // 将当前的缓冲区上限设置到当前写的位置。
    current.position(codec == null ? 0 : HEADER_SIZE);  // 当前指针修改到有效数据开始位置
  }

  /***
   * 将一个 byte 写入 current 流
   * @param i byte 字节数据
   * @throws IOException
   */
  @Override
  public void write(int i) throws IOException {
    if (current == null) {
      // 分配一个ByteBuffer给current
      getNewInputBuffer();
    }

    if (current.remaining() < 1) {
      // 表示没有剩余空间可以写入数据，
      // 将当前 current 数据刷出
      spill();
    }

    uncompressedBytes += 1;
    current.put((byte) i);
  }

  /****
   * 一次性写出去length个byte到当前的缓冲区
   * @param bytes     the data.
   * @param offset   the start offset in the data.
   * @param length   the number of bytes to write.
   * @throws IOException
   */
  @Override
  public void write(byte[] bytes, int offset, int length) throws IOException {
    if (current == null) {
      // 如果当前 current 没有byteBuffer,
      // 则分配一个 ByteBuffer 给他
      getNewInputBuffer();
    }
    // 当前可以写入的数据长度
    int remaining = Math.min(current.remaining(), length);
    current.put(bytes, offset, remaining);
    uncompressedBytes += remaining;

    // 如果没有写完，表示空间不足，需要重新分配写出
    length -= remaining;
    while (length != 0) {
      spill();
      offset += remaining;
      remaining = Math.min(current.remaining(), length);
      current.put(bytes, offset, remaining);
      uncompressedBytes += remaining;
      length -= remaining;
    }
  }

  /***
   * current 数据刷出
   *   如果是数据配置不压缩，则直接将current 内容写出
   *   如果数据配置压缩，则将 current 内容先压缩给 comparess:
   *       如果 compress 内容满则输出
   *       如果压缩后数据长度比压缩前还长，则先将compress内容刷出，在将 current 直接写出，不经过压缩
   * @throws java.io.IOException
   */
  private void spill() throws java.io.IOException {
    // 如果当前缓冲区没有任何东西，请不要溢出
    if (current == null ||
        current.position() == (codec == null ? 0 : HEADER_SIZE)) {
      return;
    }
    // 开启current 读模式
    flip();

    if (codec == null) {
      // 将数据写出
      outputBuffer(current);
      // current指向新的buffer
      getNewInputBuffer();
    } else {
      if (compressed == null) {
        // 分配压缩流
        compressed = getNewOutputBuffer();
      } else if (overflow == null) {
        overflow = getNewOutputBuffer();
      }
      // 记录初始位置
      int sizePosn = compressed.position();
      // 前移动一个 header 大小
      compressed.position(compressed.position() + HEADER_SIZE);

      // 将数据压缩
      if (codec.compress(current, compressed, overflow, options)) {
        // move position back to after the header
        // 初始化 current 流
        // 数据完全压缩完成， 初始化输出流
        uncompressedBytes = 0;
        current.position(HEADER_SIZE);
        current.limit(current.capacity());

        // find the total bytes in the chunk
        // 计算压缩的总的数据流长度
        int totalBytes = compressed.position() - sizePosn - HEADER_SIZE;
        // 数据太长可能有部分位于 overflow 中
        if (overflow != null) {
          totalBytes += overflow.position();
        }
        // 记录当前压缩内容长度
        compressedBytes += totalBytes + HEADER_SIZE;
        // 填充压缩首部到压缩流中
        writeHeader(compressed, sizePosn, totalBytes, false);
        // if we have less than the next header left, spill it.
        // 表示剩余空间不足写一个压缩首部，则准备数据刷出去
        if (compressed.remaining() < HEADER_SIZE) {
          compressed.flip();
          outputBuffer(compressed);
          compressed = overflow;
          overflow = null;
        }
      } else {

        // 如果压缩后内容不如压缩以前数据量少，则直接落地原始数据
        compressedBytes += uncompressedBytes + HEADER_SIZE;
        uncompressedBytes = 0;

        if (sizePosn != 0) {
          // 表示压缩缓冲区里面已经有数据
          // 先将压缩缓冲区里面的内容刷出去
          compressed.position(sizePosn);
          compressed.flip();
          outputBuffer(compressed);
          compressed = null;
          // if we have an overflow, clear it and make it the new compress buffer
          if (overflow != null) {
            overflow.clear();
            compressed = overflow;
            overflow = null;
          }
        } else {
          compressed.clear();
          if (overflow != null) {
            overflow.clear();
          }
        }

        // 将current内容刷出去
        // now add the current buffer into the done list and get a new one.
        current.position(0);
        // update the header with the current length
        writeHeader(current, 0, current.limit() - HEADER_SIZE, true);
        outputBuffer(current);
        getNewInputBuffer();
      }
    }
  }

  @Override
  public void getPosition(PositionRecorder recorder) {
    if (codec == null) {
      recorder.addPosition(uncompressedBytes);
    } else {
      // 写入压缩的数据量
      recorder.addPosition(compressedBytes);
      // 写入当前未压缩的数据量
      recorder.addPosition(uncompressedBytes);
    }
  }

  @Override
  public void flush() throws IOException {
    spill();  // current 内容刷出

    // 如果 compressed 有数据
    if (compressed != null && compressed.position() != 0) {
      compressed.flip();
      outputBuffer(compressed);
    }

    if (cipher != null) {
      finishEncryption();
    }
    compressed = null;
    uncompressedBytes = 0;
    compressedBytes = 0;
    overflow = null;
    current = null;
  }

  @Override
  public String toString() {
    return name.toString();
  }

  @Override
  public long getBufferSize() {
    if (codec == null) {
      // 只记录 current 的空间长度
      return uncompressedBytes + (current == null ? 0 : current.remaining());
    } else {
      // current + compressed + overflow 长度
      long result = 0;
      if (current != null) {
        result += current.capacity();
      }
      if (compressed != null) {
        result += compressed.capacity();
      }
      if (overflow != null) {
        result += overflow.capacity();
      }
      return result + compressedBytes;
    }
  }

  /**
   * Set suppress flag
   */
  public void suppress() {
    receiver.suppress();
  }
}

