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

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionCodec;
import org.apache.orc.EncryptionVariant;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.PhysicalWriter;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.writer.StreamOptions;
import org.apache.orc.impl.writer.WriterEncryptionKey;
import org.apache.orc.impl.writer.WriterEncryptionVariant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class PhysicalFsWriter implements PhysicalWriter {
  private static final Logger LOG = LoggerFactory.getLogger(PhysicalFsWriter.class);

  private static final int HDFS_BUFFER_SIZE = 256 * 1024;

  private FSDataOutputStream rawWriter;         // HDFS 写入工具类
  private final DirectStream rawStream;         // 直接落地HDFS的写入流

  // the compressed metadata information outStream
  private OutStream compressStream;             // 用于压缩的消息流
  // a protobuf outStream around streamFactory
  private CodedOutputStream codedCompressStream;  // 用于将 prorobuf 写入HDFS的工具流

  private Path path;                         // ORC 写出的数据位置
  private final HadoopShims shims;           // hadoop 操作句柄
  private final long blockSize;              // HDFS 块大小
  private final int maxPadding;
  private final StreamOptions compress;      // 流压缩选项
  private final OrcFile.CompressionStrategy compressionStrategy;  // 压缩策略

  private final boolean addBlockPadding;     // 是否填充Block边界
  private final boolean writeVariableLengthBlocks;   // 是否可以写入可变长度的Block
  private final VariantTracker unencrypted;  // 未加密的流存储

  private long headerLength;                 //  ORC header 长度
  private long stripeStart;
  private long blockOffset;                  // 上次我们写了一个短块的位置，它变成了自然块
  private int metadataLength;                // metadata 长度
  private int stripeStatisticsLength = 0;    // 加密的stripeStatistics 长度
  private int footerLength;                  // footer 长度
  private int stripeNumber = 0;              // stripe 数量

  private final Map<WriterEncryptionVariant, VariantTracker> variants = new TreeMap<>();  // 加密列的信息

  public PhysicalFsWriter(FileSystem fs,
                          Path path,
                          OrcFile.WriterOptions opts
                          ) throws IOException {
    this(fs, path, opts, new WriterEncryptionVariant[0]);
  }

  public PhysicalFsWriter(FileSystem fs,
                          Path path,
                          OrcFile.WriterOptions opts,
                          WriterEncryptionVariant[] encryption
                          ) throws IOException {
    this(fs.create(path,                          // 写入路径
                  opts.getOverwrite(),            // 是否覆盖写
                  HDFS_BUFFER_SIZE,               // HDFS Buffer 大小
                  fs.getDefaultReplication(path), // 副本数量
                  opts.getBlockSize()             // Block大小
          ),
          opts,
          encryption
    );

    this.path = path;
    LOG.info("ORC writer created for path: {} with stripeSize: {} blockSize: {}" +
            " compression: {}", path, opts.getStripeSize(), blockSize, compress);
  }

  /****
   *
   * @param outputStream fs.create(path,....) 构建的HDFS写出流
   * @param opts         写选项
   * @param encryption
   * @throws IOException
   */
  public PhysicalFsWriter(FSDataOutputStream outputStream,
                          OrcFile.WriterOptions opts,
                          WriterEncryptionVariant[] encryption
                          ) throws IOException {

    this.rawWriter = outputStream;                   // HDFS 写入工具类
    long defaultStripeSize = opts.getStripeSize();   // 基于配置获取默认的stripe大小 {orc.stripe.size}
    this.addBlockPadding = opts.getBlockPadding();   // 基于配置获取是否填充block {orc.block.padding}

    // 设置压缩流选项
    if (opts.isEnforceBufferSize()) {
      this.compress = new StreamOptions(opts.getBufferSize());
    } else {
      int estimateBufferSize =  WriterImpl.getEstimatedBufferSize(defaultStripeSize, opts.getSchema().getMaximumId() + 1, opts.getBufferSize());
      this.compress = new StreamOptions(estimateBufferSize);
    }

    // 获取压缩算法 {orc.compress}
    // 支持 NONE, ZLIB, SNAPPY, LZO, LZ4, ZSTD
    CompressionCodec codec = OrcCodecPool.getCodec(opts.getCompress());
    if (codec != null){
      compress.withCodec(codec, codec.getDefaultOptions());
    }

    // 获取流压缩策略 {orc.compression.strategy}
    this.compressionStrategy = opts.getCompressionStrategy();
    this.maxPadding = (int) (opts.getPaddingTolerance() * defaultStripeSize);
    this.blockSize = opts.getBlockSize();

    blockOffset = 0;
    unencrypted = new VariantTracker(opts.getSchema(), compress);
    writeVariableLengthBlocks = opts.getWriteVariableLengthBlocks();
    shims = opts.getHadoopShims();
    rawStream = new DirectStream(rawWriter);
    compressStream = new OutStream("stripe footer", compress, rawStream);  // stripe footer 压缩流
    codedCompressStream = CodedOutputStream.newInstance(compressStream);          // 不知道干嘛用

    for(WriterEncryptionVariant variant: encryption) {
      WriterEncryptionKey key = variant.getKeyDescription();
      StreamOptions encryptOptions =
          new StreamOptions(unencrypted.options)
              .withEncryption(key.getAlgorithm(), variant.getFileFooterKey());
      variants.put(variant, new VariantTracker(variant.getRoot(), encryptOptions));
    }
  }

  /**
   * 记录有关每个列加密变量的信息。
   * 未加密数据和每个加密列根是变体。
   * Record the information about each column encryption variant.
   * The unencrypted data and each encrypted column root are variants.
   */
  protected static class VariantTracker {
    // the streams that make up the current stripe
    protected final Map<StreamName, BufferedStream> streams = new TreeMap<>();
    private final int rootColumn;
    private final int lastColumn;
    protected final StreamOptions options;

    protected final List<OrcProto.ColumnStatistics>[] stripeStats;                   // 所有stripe的所有列的统计信息

    protected final List<OrcProto.Stream> stripeStatsStreams = new ArrayList<>();
    protected final OrcProto.ColumnStatistics[] fileStats;                           // 文件级别的列状态

    VariantTracker(TypeDescription schema, StreamOptions options) {
      rootColumn = schema.getId();
      lastColumn = schema.getMaximumId();
      this.options = options;
      stripeStats = new List[schema.getMaximumId() - schema.getId() + 1];
      for(int i=0; i < stripeStats.length; ++i) {
        stripeStats[i] = new ArrayList<>();
      }
      fileStats = new OrcProto.ColumnStatistics[stripeStats.length];
    }

    /***
     * 创建一个Stream 并记录到 streams 的Map中
     * @param name 流名称标记
     */
    public BufferedStream createStream(StreamName name) {
      BufferedStream result = new BufferedStream();
      streams.put(name, result);
      return result;
    }

    /**
     * 基于流的类型Area获取对应流类型Area的所有流的统计相关信息
     * OrcProto.Stream:
     *  - 流类型Kind
     *  - 流对应的列标识 column
     *  - 数据流长度
     * @param area 流类型 Area
     * @param sizes 用于统计此类型流的指标信息
     * @return list OrcProto.Stream
     */
    public List<OrcProto.Stream> placeStreams(StreamName.Area area, SizeCounters sizes) {

      List<OrcProto.Stream> result = new ArrayList<>(streams.size());

      // 遍历所有的流
      for(Map.Entry<StreamName, BufferedStream> stream: streams.entrySet()) {

        StreamName name = stream.getKey();                    // 获取流名称
        BufferedStream bytes = stream.getValue();             // 获取流

        if (name.getArea() == area && !bytes.isSuppressed) {  // 如果流类型与给定类型相同，并且流没有被抑制

          // 构建流信息记录器
          OrcProto.Stream.Builder builder = OrcProto.Stream.newBuilder();

          long size = bytes.getOutputSize();      // 获取流长度
          if (area == StreamName.Area.INDEX) {    // 如果是索引流，则记录索引的字节大小
            sizes.index += size;
          } else {                                // 如果是数据流，则记录数据的字节大小
            sizes.data += size;
          }

          builder.setColumn(name.getColumn())    // 列
              .setKind(name.getKind())           // 列对应的数据类型
              .setLength(size);                  // 列对应的数据大小
            result.add(builder.build());
        }
      }
      return result;
    }

    /**
     * 将当前的 Area stream 落地HDFS
     * @param area 流名称
     * @param raw the raw stream to write to
     */
    public void writeStreams(StreamName.Area area, FSDataOutputStream raw) throws IOException {
      for(Map.Entry<StreamName, BufferedStream> stream: streams.entrySet()) {
        if (stream.getKey().getArea() == area) {
          stream.getValue().spillToDiskAndClear(raw);
        }
      }
    }

    /**
     * 汇总当前列的所有的流类型的数据大小
     *
     * @param column a column id
     * @return the total number of bytes
     */
    public long getFileBytes(int column) {
      long result = 0;
      if (column >= rootColumn && column <= lastColumn) {
        for(Map.Entry<StreamName, BufferedStream> entry: streams.entrySet()) {
          StreamName name = entry.getKey();                // 拿到流名称

          if (name.getColumn() == column &&
              name.getArea() != StreamName.Area.INDEX) {   // 要剔除掉 Index 类型
            result += entry.getValue().getOutputSize();
          }
        }
      }
      return result;
    }
  }

  VariantTracker getVariant(EncryptionVariant column) {
    if (column == null) {
      return unencrypted;
    }
    return variants.get(column);
  }

  /**
   * Get the number of bytes for a file in a given column
   * by finding all the streams (not suppressed)
   * for a given column and returning the sum of their sizes.
   * excludes index
   *
   * @param column column from which to get file size
   * @return number of bytes for the given column
   */
  @Override
  public long getFileBytes(int column, WriterEncryptionVariant variant) {
    return getVariant(variant).getFileBytes(column);
  }

  /***
   * 获取不加密的写出流的流选项
   */
  @Override
  public StreamOptions getStreamOptions() {
    return unencrypted.options;
  }

  // 0 填充字节流
  private static final byte[] ZEROS = new byte[64*1024];

  // 向输出流中填充0字节流
  private static void writeZeros(OutputStream output, long remaining) throws IOException {

    while (remaining > 0) {
      long size = Math.min(ZEROS.length, remaining);
      output.write(ZEROS, 0, (int) size);
      remaining -= size;
    }
  }

  /**
   * 基于当前HDFS判断是否填充当前的Block
   * 这在写入当前stripe之前调用。
   * @param stripeSize the number of bytes in the current stripe
   */
  private void padStripe(long stripeSize) throws IOException {
    // 记录写当前stripe时的位置
    this.stripeStart = rawWriter.getPos();

    // 判断当前上一个stripe在同一个block中剩余多少个字节
    long previousBytesInBlock = (stripeStart - blockOffset) % blockSize;

    // We only have options if this isn't the first stripe in the block
    if (previousBytesInBlock > 0) {
      if (previousBytesInBlock + stripeSize >= blockSize) {  // 表示 这个stripe 写进来会跨block

        if (writeVariableLengthBlocks && shims.endVariableLengthBlock(rawWriter)) {
          blockOffset = stripeStart;
        } else if (addBlockPadding) {
          // 表示需要填充的Block的大小
          long padding = blockSize - previousBytesInBlock;
          if (padding <= maxPadding) {
            writeZeros(rawWriter, padding);
            stripeStart += padding;
          }
        }
      }
    }
  }

  /**
   * An output receiver that writes the ByteBuffers to the output stream
   * as they are received.
   */
  private static class DirectStream implements OutputReceiver {
    private final FSDataOutputStream output;

    DirectStream(FSDataOutputStream output) {
      this.output = output;
    }

    @Override
    public void output(ByteBuffer buffer) throws IOException {
      output.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
          buffer.remaining());
    }

    @Override
    public void suppress() {
      throw new UnsupportedOperationException("Can't suppress direct stream");
    }
  }

  /***
   * 将 StripeFooter 写出到 HDFS,
   * 并在StripeInformation中记录Stripe开始位置与StripeFooter长度的相关记录
   * @param footer
   * @param sizes
   * @param dirEntry
   * @throws IOException
   */
  private void writeStripeFooter(OrcProto.StripeFooter footer,
                                 SizeCounters sizes,  // 记录当前 stripe 的 索引与数据长度
                                 OrcProto.StripeInformation.Builder dirEntry) throws IOException {

    // Footer 刷出到 HDFS
    footer.writeTo(codedCompressStream);
    codedCompressStream.flush();
    compressStream.flush();

    dirEntry.setOffset(stripeStart);                                               // 在当前 StripeInformation中记录 stripe的起始位置
    dirEntry.setFooterLength(rawWriter.getPos() - stripeStart - sizes.total());  // 在当前 stripeInformation中记录 footer的长度
  }

  /**
   * Write the saved encrypted stripe statistic in a variant out to the file.
   * The streams that are written are added to the tracker.stripeStatsStreams.
   * @param output the file we are writing to
   * @param stripeNumber the number of stripes in the file
   * @param tracker the variant to write out
   */
  static void writeEncryptedStripeStatistics(DirectStream output,
                                             int stripeNumber,
                                             VariantTracker tracker) throws IOException {

    StreamOptions options = new StreamOptions(tracker.options);
    tracker.stripeStatsStreams.clear();  // 清空 tracker 中的stripe 信息

    for(int col = tracker.rootColumn;
        col < tracker.rootColumn + tracker.stripeStats.length; ++col) {

      options.modifyIv(CryptoUtils.modifyIvForStream(col,
          OrcProto.Stream.Kind.STRIPE_STATISTICS, stripeNumber + 1));


      OutStream stream = new OutStream("stripe stats for " + col, options, output);

      OrcProto.ColumnarStripeStatistics stats =
          OrcProto.ColumnarStripeStatistics.newBuilder()
              .addAllColStats(tracker.stripeStats[col - tracker.rootColumn])
              .build();
      long start = output.output.getPos();
      stats.writeTo(stream);
      stream.flush();
      OrcProto.Stream description = OrcProto.Stream.newBuilder()
                                   .setColumn(col)
                                   .setKind(OrcProto.Stream.Kind.STRIPE_STATISTICS)
                                   .setLength(output.output.getPos() - start)
                                   .build();
      tracker.stripeStatsStreams.add(description);
    }
  }

  /**
   * 将所有stripe的所有的列统计信息载入Metadata结构中
   * @param builder     Metadata
   * @param stripeCount Stripe 数量
   * @param stats       所有 stripe 的所有列的统计指标
   */
  static void setUnencryptedStripeStatistics(OrcProto.Metadata.Builder builder,
                                             int stripeCount,
                                             List<OrcProto.ColumnStatistics>[] stats) {

    // 清空所有的 StripeStatics
    builder.clearStripeStats();

    // 遍历所有stripe
    for(int s=0; s < stripeCount; ++s) {
      // 填充当前 stripe 的所有列的 columnstatistics
      OrcProto.StripeStatistics.Builder stripeStats = OrcProto.StripeStatistics.newBuilder();
      for(List<OrcProto.ColumnStatistics> col: stats) {     //
        stripeStats.addColStats(col.get(s));                // 压入当前 stripe 的列统计指标
      }
      builder.addStripeStats(stripeStats.build());          //  Metadata 中压入 stripeStatics 统计指标
    }
  }

  static void setEncryptionStatistics(OrcProto.Encryption.Builder encryption,
                                      int stripeNumber,
                                      Collection<VariantTracker> variants
                                      ) throws IOException {
    int v = 0;
    for(VariantTracker variant: variants) {
      OrcProto.EncryptionVariant.Builder variantBuilder =
          encryption.getVariantsBuilder(v++);

      // Add the stripe statistics streams to the variant description.
      variantBuilder.clearStripeStatistics();
      variantBuilder.addAllStripeStatistics(variant.stripeStatsStreams);

      // Serialize and encrypt the file statistics.
      OrcProto.FileStatistics.Builder file = OrcProto.FileStatistics.newBuilder();
      for(OrcProto.ColumnStatistics col: variant.fileStats) {
        file.addColumn(col);
      }
      StreamOptions options = new StreamOptions(variant.options);
      options.modifyIv(CryptoUtils.modifyIvForStream(variant.rootColumn,
          OrcProto.Stream.Kind.FILE_STATISTICS, stripeNumber + 1));
      BufferedStream buffer = new BufferedStream();
      OutStream stream = new OutStream("stats for " + variant, options, buffer);
      file.build().writeTo(stream);
      stream.flush();
      variantBuilder.setFileStatistics(buffer.getBytes());
    }
  }


  /***
   * 写入加密stripe与FileMetadata的统计信息
   * @param builder Metadata builder to finalize and write.
   * @throws IOException
   */
  @Override
  public void writeFileMetadata(OrcProto.Metadata.Builder builder) throws IOException {

    long stripeStatisticsStart = rawWriter.getPos();  //获取 stripeStatisticsStart 开始位置（当前要写入的位置）

    // 写加密列信息
    for(VariantTracker variant: variants.values()) {
      writeEncryptedStripeStatistics(rawStream, stripeNumber, variant);
    }

    // 填充当前的Metadata信息
    // 将所有stripe的所有列的stripe级别的columnstatistics填充到 Metadata
    setUnencryptedStripeStatistics(builder, stripeNumber, unencrypted.stripeStats);

    // 记录metadata 开始位置
    long metadataStart = rawWriter.getPos();

    // 将 metadata 写入 HDFS
    builder.build().writeTo(codedCompressStream);
    codedCompressStream.flush();
    compressStream.flush();

    this.stripeStatisticsLength = (int) (metadataStart - stripeStatisticsStart);  // 记录加密 stripe的长度
    this.metadataLength = (int) (rawWriter.getPos() - metadataStart);             // 记录metadata的长度
  }

  /***
   * 将列的统计信息写入Footer
   * @param builder 要填充的Footer
   * @param stats 列统计信息 statics
   */
  static void addUnencryptedStatistics(OrcProto.Footer.Builder builder,
                                       OrcProto.ColumnStatistics[] stats) {
    for(OrcProto.ColumnStatistics stat: stats) {
      builder.addStatistics(stat);
    }
  }

  /***
   * 填充 Footer 的剩余信息， 包含数据体长度，首部长度，还有文件级别的columnstatistics
   * 将 Footer 内容写出到文件
   * @param builder Footer builder to finalize and write.
   * @throws IOException
   */
  @Override
  public void writeFileFooter(OrcProto.Footer.Builder builder) throws IOException {

    if (variants.size() > 0) {
      OrcProto.Encryption.Builder encryption = builder.getEncryptionBuilder();
      setEncryptionStatistics(encryption, stripeNumber, variants.values());
    }
    // 填充文件级别的每个列的 statics
    addUnencryptedStatistics(builder, unencrypted.fileStats);
    // 记录body体的长度 ： 当前位置-metadata长度-加密stipe的长度
    long bodyLength = rawWriter.getPos() - metadataLength - stripeStatisticsLength;

    builder.setContentLength(bodyLength);   // Footer 中记录Body的长度
    builder.setHeaderLength(headerLength);  // Footer 中记录魔数的长度

    // 记录开始写Footer的位置，并写入Footer
    long startPosn = rawWriter.getPos();
    OrcProto.Footer footer = builder.build();
    footer.writeTo(codedCompressStream);
    codedCompressStream.flush();
    compressStream.flush();

    // 记录Footer的长度
    this.footerLength = (int) (rawWriter.getPos() - startPosn);
  }

  /***
   * 填充 PostScript 的剩余部分内容
   * 包含 Footer 部分长度与 metadata 长度
   * 将 PostScript 部分写入HDFS
   * @param builder Postscript 结构填充
   * @throws IOException
   * @return 返回当前写入的最后位置
   */
  @Override
  public long writePostScript(OrcProto.PostScript.Builder builder) throws IOException {
    builder.setFooterLength(footerLength);     // 记录当前Footer的长度
    builder.setMetadataLength(metadataLength); // 记录metadata的长度

    // 记录当前加密stripe信息的长度
    if (variants.size() > 0) {
      builder.setStripeStatisticsLength(stripeStatisticsLength);
    }

    // 记录并将 postscirpt 写入文件
    OrcProto.PostScript ps = builder.build();
    long startPosn = rawWriter.getPos();
    ps.writeTo(rawWriter);

    // 记录postscript的长度
    long length = rawWriter.getPos() - startPosn;

    // postScript 最终长度不超过一个1字节， 所以最后一位将保存PostScript 的长度
    if (length > 255) {
      throw new IllegalArgumentException("PostScript too large at " + length);
    }
    // 写入postscript长度
    rawWriter.writeByte((int)length);

    return rawWriter.getPos();
  }

  /***
   * 关闭当前的HDFS写入工具
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    // 我们不直接使用编解码器，但在getCompressionCodec；中提供编解码器；
    // that is used in tests, for boolean checks, and in StreamFactory.
    // 用于测试、布尔检查和StreamFactory中。
    // Some of the changes that would get rid of this pattern require cross-project interface changes, so just return the codec for now.
    CompressionCodec codec = compress.getCodec();
    if (codec != null) {
      OrcCodecPool.returnCodec(codec.getKind(), codec);
    }
    compress.withCodec(null, null);
    rawWriter.close();
    rawWriter = null;
  }

  @Override
  public void flush() throws IOException {
    rawWriter.hflush();
  }

  /***
   * 向HDFS写入一个 stripe 并将stripe的开始位置记录到dirEntry
   * @param buffer Stripe data buffer.
   * @param dirEntry File metadata entry for the stripe, to be updated with relevant data.
   * @throws IOException
   */
  @Override
  public void appendRawStripe(ByteBuffer buffer,
      OrcProto.StripeInformation.Builder dirEntry) throws IOException {
    long start = rawWriter.getPos();    // 记录开始写入位置
    int length = buffer.remaining();    // 记录当前 stripe 的长度

    long availBlockSpace = blockSize - (start % blockSize);  // 计算当前Block的空余量

    // 如果当前stripe 需要跨block 写，而且长度不足一个blcok,则填充当前的block
    if (length < blockSize && length > availBlockSpace && addBlockPadding) {
      byte[] pad = new byte[(int) Math.min(HDFS_BUFFER_SIZE, availBlockSpace)];
      LOG.info("Padding ORC by {} bytes while merging", availBlockSpace);
      start += availBlockSpace;
      while (availBlockSpace > 0) {
        int writeLen = (int) Math.min(availBlockSpace, pad.length);
        rawWriter.write(pad, 0, writeLen);
        availBlockSpace -= writeLen;
      }
    }
    // 将当前的stripe写入HDFS
    rawWriter.write(buffer.array(), buffer.arrayOffset() + buffer.position(), length);
    dirEntry.setOffset(start);  // 记录当前的 stripe位置
    stripeNumber += 1;          // stripe 数量迭代一个
  }


  /**
   * This class is used to hold the contents of streams as they are buffered.
   * The TreeWriters write to the outStream and the codec compresses the data as buffers fill up and stores them in the output list.
   * When the stripe is being written, the whole stream is written to the file.
   */
  static final class BufferedStream implements OutputReceiver {
    private boolean isSuppressed = false;
    private final List<ByteBuffer> output = new ArrayList<>();

    @Override
    public void output(ByteBuffer buffer) {
      if (!isSuppressed) {
        output.add(buffer);
      }
    }

    @Override
    public void suppress() {
      isSuppressed = true;
      output.clear();
    }

    /**
     * 如果需要，将任何保存的缓冲区写入OutputStream，并清除所有缓冲区。
     * @return true if the stream was written
     */
    boolean spillToDiskAndClear(FSDataOutputStream raw) throws IOException {
      if (!isSuppressed) {
        for (ByteBuffer buffer: output) {
          raw.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
            buffer.remaining());
        }
        output.clear();
        return true;
      }
      isSuppressed = false;
      return false;
    }

    /**
     * Get the buffer as a protobuf ByteString and clears the BufferedStream.
     * @return the bytes
     */
    ByteString getBytes() {
      int len = output.size();
      if (len == 0) {
        return ByteString.EMPTY;
      } else {
        ByteString result = ByteString.copyFrom(output.get(0));
        for (int i=1; i < output.size(); ++i) {
          result = result.concat(ByteString.copyFrom(output.get(i)));
        }
        output.clear();
        return result;
      }
    }

    /**
     * Get the stream as a ByteBuffer and clear it.
     * @return a single ByteBuffer with the contents of the stream
     */
    ByteBuffer getByteBuffer() {
      ByteBuffer result;
      if (output.size() == 1) {
        result = output.get(0);
      } else {
        result = ByteBuffer.allocate((int) getOutputSize());
        for (ByteBuffer buffer : output) {
          result.put(buffer);
        }
        output.clear();
        result.flip();
      }
      return result;
    }

    /**
     * Get the number of bytes that will be written to the output.
     *
     * Assumes the stream writing into this receiver has already been flushed.
     * @return number of bytes
     */
    public long getOutputSize() {
      long result = 0;
      for (ByteBuffer buffer: output) {
        result += buffer.remaining();
      }
      return result;
    }
  }



  static class SizeCounters {
    long index = 0;
    long data = 0;

    long total() {
      return index + data;
    }
  }

  /***
   * 基于当前的流信息填充FooterBuilder
   * @param footerBuilder
   * @param sizes
   * @throws IOException
   */
  void buildStreamList(OrcProto.StripeFooter.Builder footerBuilder,
                       SizeCounters sizes) throws IOException {

    // 填充 Index 类型的流数据信息
    //      case ROW_INDEX:
    //      case DICTIONARY_COUNT:
    //      case BLOOM_FILTER:
    //      case BLOOM_FILTER_UTF8:
    //      case ENCRYPTED_INDEX:
    footerBuilder.addAllStreams(
        unencrypted.placeStreams(StreamName.Area.INDEX, sizes));

    // 统计索引流大小
    final long unencryptedIndexSize = sizes.index;

    int v = 0;
    for (VariantTracker variant: variants.values()) {
      OrcProto.StripeEncryptionVariant.Builder builder = footerBuilder.getEncryptionBuilder(v++);
      builder.addAllStreams(variant.placeStreams(StreamName.Area.INDEX, sizes));
    }

    // 添加加密流类型
    if (sizes.index != unencryptedIndexSize) {
      // add a placeholder that covers the hole where the encrypted indexes are
      footerBuilder.addStreams(OrcProto.Stream.newBuilder()
                                   .setKind(OrcProto.Stream.Kind.ENCRYPTED_INDEX)
                                   .setLength(sizes.index - unencryptedIndexSize));
    }

    // 添加data类型的索引信息
    footerBuilder.addAllStreams(unencrypted.placeStreams(StreamName.Area.DATA, sizes));

    final long unencryptedDataSize = sizes.data;
    v = 0;
    for (VariantTracker variant: variants.values()) {
      OrcProto.StripeEncryptionVariant.Builder builder = footerBuilder.getEncryptionBuilder(v++);
      builder.addAllStreams(variant.placeStreams(StreamName.Area.DATA, sizes));
    }

    if (sizes.data != unencryptedDataSize) {
      // add a placeholder that covers the hole where the encrypted indexes are
      footerBuilder.addStreams(OrcProto.Stream.newBuilder()
                                   .setKind(OrcProto.Stream.Kind.ENCRYPTED_DATA)
                                   .setLength(sizes.data - unencryptedDataSize));
    }
  }

  /***
   * 完成一个stripe的构建:
   *  1. 完成 StripeFooter 的 stream 填充
   *  2. 将 INDEX 类型的 STREAM 刷写到 HDFS
   *  3. 将 DATA 类型的 STREAM 刷写到 HDFS
   *  4. 将 StripeFooter 刷写到 HDFS
   *  5. 完善填充 stripe 信息
   * @param footerBuilder Stripe footer to be updated with relevant data and written out.
   * @param dirEntry File metadata entry for the stripe, to be updated with relevant data.
   * @throws IOException
   */
  @Override
  public void finalizeStripe(OrcProto.StripeFooter.Builder footerBuilder,
                             OrcProto.StripeInformation.Builder dirEntry) throws IOException {

    SizeCounters sizes = new SizeCounters();

    // 填充 StripeFooter 的 stream 字段
    buildStreamList(footerBuilder, sizes);
    OrcProto.StripeFooter footer = footerBuilder.build();

    // 我们需要stripe文件，使条纹不跨越块边界吗
    padStripe(sizes.total() + footer.getSerializedSize());

    // 将 INDEX 内容写出到HDFS
    unencrypted.writeStreams(StreamName.Area.INDEX, rawWriter);
    for (VariantTracker variant: variants.values()) {
      variant.writeStreams(StreamName.Area.INDEX, rawWriter);
    }

    // 将DATA写出到HDFS
    unencrypted.writeStreams(StreamName.Area.DATA, rawWriter);
    for (VariantTracker variant: variants.values()) {
      variant.writeStreams(StreamName.Area.DATA, rawWriter);
    }

    // 将 Footer 内容写出到 HDFS
    writeStripeFooter(footer, sizes, dirEntry);

    // fill in the data sizes
    dirEntry.setDataLength(sizes.data);
    dirEntry.setIndexLength(sizes.index);

    stripeNumber += 1;
  }

  /***
   * 写 ORC 头部信息
   * @throws IOException
   */
  @Override
  public void writeHeader() throws IOException {
    rawWriter.writeBytes(OrcFile.MAGIC);
    headerLength = rawWriter.getPos();  //
  }

  /***
   * 创建一个缓冲类型的OutputReceiver
   * @param name the name of the stream
   * @return
   */
  @Override
  public BufferedStream createDataStream(StreamName name) {
    VariantTracker variant = getVariant(name.getEncryption());
    BufferedStream result = variant.streams.get(name);
    if (result == null) {
      result = new BufferedStream();
      variant.streams.put(name, result);
    }
    return result;
  }

  /***
   * 基于类型获取流类型选项
   */
  private StreamOptions getOptions(OrcProto.Stream.Kind kind) {
    return SerializationUtils.getCustomizedCodec(compress, compressionStrategy, kind);
  }

  /****
   * 获取索引流类型，他是一个基于字节流的缓冲流
   * @param name
   * @return
   */
  protected OutputStream createIndexStream(StreamName name) {

    BufferedStream buffer = createDataStream(name);
    VariantTracker tracker = getVariant(name.getEncryption());

    StreamOptions options =
        SerializationUtils.getCustomizedCodec(tracker.options, compressionStrategy, name.getKind());

    if (options.isEncrypted()) {
      if (options == tracker.options) {
        options = new StreamOptions(options);
      }
      options.modifyIv(CryptoUtils.modifyIvForStream(name, stripeNumber + 1));
    }
    return new OutStream(name.toString(), options, buffer);
  }

  /***
   * 将索引写入 IndexStream
   * @param name stream 名称
   * @param index 要写入的index信息
   * @throws IOException
   */
  @Override
  public void writeIndex(StreamName name,
                         OrcProto.RowIndex.Builder index) throws IOException {
    OutputStream stream = createIndexStream(name);
    index.build().writeTo(stream);
    stream.flush();
  }

  /***
   * 将Bloom写入 index stream
   * @param name 流名称
   * @param bloom 要写入的BloomFilter信息
   * @throws IOException
   */
  @Override
  public void writeBloomFilter(StreamName name,
                               OrcProto.BloomFilterIndex.Builder bloom) throws IOException {
    OutputStream stream = createIndexStream(name);
    bloom.build().writeTo(stream);
    stream.flush();
  }

  /***
   * 将 CloumnStatistics 写入到流管理中
   * @param name the name of the stream
   * @param statistics the statistics to write
   */
  @Override
  public void writeStatistics(StreamName name,
                              OrcProto.ColumnStatistics.Builder statistics) {
    VariantTracker tracker = getVariant(name.getEncryption());  // 获取对应的流管理

    if (name.getKind() == OrcProto.Stream.Kind.FILE_STATISTICS) {
      tracker.fileStats[name.getColumn() - tracker.rootColumn] = statistics.build();
    } else {
      tracker.stripeStats[name.getColumn() - tracker.rootColumn].add(statistics.build());
    }
  }

  @Override
  public String toString() {
    if (path != null) {
      return path.toString();
    } else {
      return ByteString.EMPTY.toString();
    }
  }
}
