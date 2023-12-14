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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.orc.EncryptionAlgorithm;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

public interface HadoopShims {

  // 压缩类型
  enum DirectCompressionType {
    NONE,
    ZLIB_NOHEADER,
    ZLIB,
    SNAPPY,
  }

  // 压缩器
  interface DirectDecompressor {
    // 解压方法
    void decompress(ByteBuffer var1, ByteBuffer var2) throws IOException;
    void reset();
    void end();
  }

  /**
   * 基于压缩类型获取压缩器
   * 获取直接解压器编解码器（如果可用）
   * @param codec the kind of decompressor that we need 压缩类型 {@link DirectCompressionType}
   * @return a direct decompressor or null, if it isn't available 一个压缩器 {@link DirectDecompressor}
   */
  DirectDecompressor getDirectDecompressor(DirectCompressionType codec);

  /**
   * a hadoop.io ByteBufferPool shim.
   */
  interface ByteBufferPoolShim {

    /**
     * 从 pool 里拿一个新的 ByteBuffer。
     * pool 可以通过从其内部缓存中删除缓冲区或分配新缓冲区来提供此功能。
     *
     * @param direct     Whether the buffer should be direct.
     * @param length     The minimum length the buffer will have.
     * @return           A new ByteBuffer. Its capacity can be less than what was requested, but must be at least 1 byte.
     */
    ByteBuffer getBuffer(boolean direct, int length);

    /**
     * 将 buffer 释放回 pool.
     * pool 可以选择将该buffer放入其cache/free。
     * The pool may choose to put this buffer into its cache/free it.
     *
     * @param buffer    a direct bytebuffer
     */
    void putBuffer(ByteBuffer buffer);
  }

  /**
   * 提供 HDFS 零拷贝的工具类 -> ZeroCopyReaderShim
   * @param in FSDataInputStream to read from (where the cached/mmap buffers are tied to)
   * @param pool ByteBufferPoolShim to allocate fallback buffers with
   * @return returns null if not supported
   */
  ZeroCopyReaderShim getZeroCopyReader(FSDataInputStream in, ByteBufferPoolShim pool) throws IOException;

  // HDFS 零拷贝封装类
  interface ZeroCopyReaderShim extends Closeable {

    /**
     * 从FSDataInputStream获取ByteBuffer-这可以是 HeapByteBuffer 或 sMappedByteBufffer。
     * 同时，按该量移动入流。读取的数据可以小于 maxLength。
     *
     * @return ByteBuffer read from the stream,
     */
    ByteBuffer readBuffer(int maxLength, boolean verifyChecksums) throws IOException;

    /**
     * 释放该 ByteBuffer
     */
    void releaseBuffer(ByteBuffer buffer);

    /**
     * Close the underlying stream.
     */
    @Override
    void close() throws IOException;
  }

  /**
   * 在当前位置结束OutputStream的当前块。
   * End the OutputStream's current block at the current location.
   * This is only available on HDFS on Hadoop &ge; 2.7, but will return false otherwise.
   * 这仅适用于Hadoop上的HDFS 2.7，否则将返回false。
   *
   * @return was a variable length block created?
   */
  boolean endVariableLengthBlock(OutputStream output) throws IOException;

  /**
   * 存储类型
   * The known KeyProviders for column encryption.
   * These are identical to OrcProto.KeyProviderKind.
   */
  enum KeyProviderKind {
    UNKNOWN(0),
    HADOOP(1),
    AWS(2),    // amazon
    GCP(3),    // google
    AZURE(4);  // microsoft

    private final int value;

    KeyProviderKind(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  /**
   * Information about a crypto key including the key name, version, and the algorithm.
   * 有关加密密钥的信息，包括密钥名称、版本和算法。
   */
  class KeyMetadata {
    private final String keyName;    // 密钥名称
    private final int version;       // 密钥版本
    private final EncryptionAlgorithm algorithm;  // 加密算法， 当前只只是AES加密算法

    public KeyMetadata(String key, int version, EncryptionAlgorithm algorithm) {
      this.keyName = key;
      this.version = version;
      this.algorithm = algorithm;
    }

    /**
     * Get the name of the key.
     */
    public String getKeyName() {
      return keyName;
    }

    /**
     * Get the encryption algorithm for this key.
     * @return the algorithm
     */
    public EncryptionAlgorithm getAlgorithm() {
      return algorithm;
    }

    /**
     * Get the version of this key.
     * @return the version
     */
    public int getVersion() {
      return version;
    }

    @Override
    public String toString() {
      return keyName + '@' + version + ' ' + algorithm;
    }
  }

  /**
   * Create a Hadoop KeyProvider to get encryption keys.
   * @param conf the configuration
   * @param random a secure random number generator
   * @return a key provider or null if none was provided
   */
  KeyProvider getHadoopKeyProvider(Configuration conf,
                                   Random random) throws IOException;

}
