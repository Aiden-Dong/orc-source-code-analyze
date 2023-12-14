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

package org.apache.orc.impl.writer;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.BitFieldWriter;
import org.apache.orc.impl.ColumnStatisticsImpl;
import org.apache.orc.impl.CryptoUtils;
import org.apache.orc.impl.IntegerWriter;
import org.apache.orc.impl.OutStream;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.PositionedOutputStream;
import org.apache.orc.impl.RunLengthIntegerWriter;
import org.apache.orc.impl.RunLengthIntegerWriterV2;
import org.apache.orc.impl.StreamName;
import org.apache.orc.util.BloomFilter;
import org.apache.orc.util.BloomFilterIO;
import org.apache.orc.util.BloomFilterUtf8;

import java.io.IOException;
import java.util.List;

/**
 * The parent class of all of the writers for each column. Each column
 * is written by an instance of this class. The compound types (struct,
 * list, map, and union) have children tree writers that write the children
 * types.
 */
public abstract class TreeWriterBase implements TreeWriter {
  protected final int id;
  protected final BitFieldWriter isPresent;                  // 建立非空的位图向量
  protected final TypeDescription schema;
  protected final WriterEncryptionVariant encryption;        // 加密工具
  private final boolean isCompressed;

  protected final ColumnStatisticsImpl indexStatistics;       // stripe 内部行组的统计信息
                                                              // 主要包含非空值，最大值，最小值，求和等

  // stripe 级别的统计信息
  protected final ColumnStatisticsImpl stripeColStatistics;   // stripe 级别的统计信息
                                                              // 主要包含非空值，最大值，最小值，求和等
  // 文件级别的统计信息
  protected final ColumnStatisticsImpl fileStatistics;        // 文件级别的统计信息
                                                              // 主要包含空值，最大值，最小值，求和等

  private final OrcProto.RowIndexEntry.Builder rowIndexEntry; // 当前行组的统计信息

  // 索引的创建收到 orc.row.index.stride 配置的影响
  protected final RowIndexPositionRecorder rowIndexPosition;  // 行索引信息, rowIndexEntry封装类型

  private final OrcProto.RowIndex.Builder rowIndex;           // 行中每个列的索引信息

  // Bloom过滤器
  protected final BloomFilter bloomFilter;
  protected final BloomFilterUtf8 bloomFilterUtf8;
  protected final boolean createBloomFilter;
  private final OrcProto.BloomFilterIndex.Builder bloomFilterIndex;
  private final OrcProto.BloomFilterIndex.Builder bloomFilterIndexUtf8;
  protected final OrcProto.BloomFilter.Builder bloomFilterEntry;

  private boolean foundNulls;                 // 这个 Schema 中是否有空值
  private OutStream isPresentOutStream;       // ???? 给啥用的
  protected final WriterContext context;      // ???? 给啥用的

  /**
   * Create a tree writer.
   * @param schema the row schema
   * @param encryption the encryption variant or null if it is unencrypted
   * @param context limited access to the Writer's data.
   */
  TreeWriterBase(TypeDescription schema,
                 WriterEncryptionVariant encryption,
                 WriterContext context) throws IOException {

    this.schema = schema;                         // 字段类型
    this.encryption = encryption;                 // 字段加密信息
    this.context = context;                       // WriterImpl/StreamFactory
    this.isCompressed = context.isCompressed();   // 是否要压缩
    this.id = schema.getId();                     // 该 schema Id

    // 创建一个 PRESENT 类型流
    isPresentOutStream = context.createStream(new StreamName(id, OrcProto.Stream.Kind.PRESENT, encryption));
    isPresent = new BitFieldWriter(isPresentOutStream, 1);

    // 标记此Schema 是否有空值
    this.foundNulls = false;

    createBloomFilter = context.getBloomFilterColumns()[id];  // 是否创建BloomFilter
    boolean proleptic = context.getProlepticGregorian();

    // 这是相关的统计信息
    indexStatistics = ColumnStatisticsImpl.create(schema, proleptic);
    stripeColStatistics = ColumnStatisticsImpl.create(schema, proleptic);
    fileStatistics = ColumnStatisticsImpl.create(schema, proleptic);

    // 创建索引
    if (context.buildIndex()) {                                       // 是否建立索引
      rowIndex = OrcProto.RowIndex.newBuilder();
      rowIndexEntry = OrcProto.RowIndexEntry.newBuilder();
      rowIndexPosition = new RowIndexPositionRecorder(rowIndexEntry); //
    } else {
      rowIndex = null;
      rowIndexEntry = null;
      rowIndexPosition = null;
    }

    // 创建 BloomFilter
    if (createBloomFilter) {   // 该列是否创建BloomFilter
      bloomFilterEntry = OrcProto.BloomFilter.newBuilder();
      // 旧版 BloomFileter
      if (context.getBloomFilterVersion() == OrcFile.BloomFilterVersion.ORIGINAL) {
        bloomFilter = new BloomFilter(context.getRowIndexStride(), context.getBloomFilterFPP());
        bloomFilterIndex = OrcProto.BloomFilterIndex.newBuilder();
      } else {
        bloomFilter = null;
        bloomFilterIndex = null;
      }
      // UTF8 BloomFilter
      bloomFilterUtf8 = new BloomFilterUtf8(context.getRowIndexStride(), context.getBloomFilterFPP());
      bloomFilterIndexUtf8 = OrcProto.BloomFilterIndex.newBuilder();
    } else {
      bloomFilterEntry = null;
      bloomFilterIndex = null;
      bloomFilterIndexUtf8 = null;
      bloomFilter = null;
      bloomFilterUtf8 = null;
    }
  }

  protected OrcProto.RowIndex.Builder getRowIndex() {
    return rowIndex;
  }

  protected ColumnStatisticsImpl getStripeStatistics() {
    return stripeColStatistics;
  }

  protected OrcProto.RowIndexEntry.Builder getRowIndexEntry() {
    return rowIndexEntry;
  }

  IntegerWriter createIntegerWriter(PositionedOutputStream output,
                                    boolean signed,
                                    boolean isDirectV2,
                                    WriterContext writer) {
    if (isDirectV2) {
      boolean alignedBitpacking = writer.getEncodingStrategy().equals(OrcFile.EncodingStrategy.SPEED);
      return new RunLengthIntegerWriterV2(output, signed, alignedBitpacking);
    } else {
      return new RunLengthIntegerWriter(output, signed);
    }
  }

  boolean isNewWriteFormat(WriterContext writer) {
    return writer.getVersion() != OrcFile.Version.V_0_11;
  }

  /**
   * 处理顶级对象的写入。
   * 此默认方法用于除struct之外的所有类型，struct是典型的情况。
   * VectorizedRowBatch 假设顶层对象是 struct，
   * 所以我们对所有其他类型使用第一列
   * @param batch the batch to write from
   * @param offset the row to start on
   * @param length the number of rows to write
   */
  @Override
  public void writeRootBatch(VectorizedRowBatch batch, int offset, int length) throws IOException {
    writeBatch(batch.cols[0], offset, length);
  }

  /**
   * 写入指定的列向量
   * 此方法主要是对batch做空值判断并记录空值情况 :
   *   1. 向 PRESENT流中记录当前batch的空值情况
   *   2. indexStatistics 中记录非空的值的数量，并记录是否存在空值
   * @param vector the vector to write from
   * @param offset the first value from the vector to write， 要写入向量的初始值偏移
   * @param length the number of values from the vector to write 要写入向量的长度
   */
  @Override
  public void writeBatch(ColumnVector vector, int offset, int length) throws IOException {

    if (vector.noNulls) {
      // 如果没有空值的情况
      indexStatistics.increment(length);
      if (isPresent != null) {
        for (int i = 0; i < length; ++i) {
          // isPresent 记录全为非空值
          isPresent.write(1);
        }
      }
    } else {
      // 如果存在空值的情况

      if (vector.isRepeating) {
        // 假设 vector 都是重复值
        boolean isNull = vector.isNull[0];

        if (isPresent != null) {
          for (int i = 0; i < length; ++i) {
            isPresent.write(isNull ? 0 : 1);
          }
        }

        if (isNull) {
          foundNulls = true;
          indexStatistics.setNull();
        } else {
          indexStatistics.increment(length);
        }
      } else {
        // count the number of non-null values
        // 计算非空值的数量
        int nonNullCount = 0;

        for(int i = 0; i < length; ++i) {
          boolean isNull = vector.isNull[i + offset];

          if (!isNull) {
            nonNullCount += 1;
          }
          
          // isPresent 记录每个值是空或者非空的boolean向量
          if (isPresent != null) {
            isPresent.write(isNull ? 0 : 1);
          }
        }

        // 记录这个列非空值的数量
        indexStatistics.increment(nonNullCount);
        if (nonNullCount != length) {
          foundNulls = true;              //
          indexStatistics.setNull();       // 代表有空值
        }
      }
    }
  }

  private void removeIsPresentPositions() {
    for(int i=0; i < rowIndex.getEntryCount(); ++i) {  // 遍历所有的RowIndexEntry
      // 获取当前的 RowIndexEntry
      OrcProto.RowIndexEntry.Builder entry = rowIndex.getEntryBuilder(i);
      List<Long> positions = entry.getPositionsList();  // 获取当前的RowIndexEntry的位置偏移信息

      // bit streams use 3 positions if uncompressed, 4 if compressed
      positions = positions.subList(isCompressed ? 4 : 3, positions.size());
      entry.clearPositions();
      entry.addAllPositions(positions);
    }
  }

  @Override
  public void prepareStripe(int stripeId) {
    if (isPresent != null) {
      // 处理加密
      isPresent.changeIv(CryptoUtils.modifyIvForStripe(stripeId));
    }
  }

  /***
   * 刷出流
   */
  @Override
  public void flushStreams() throws IOException {
    if (isPresent != null) {
      isPresent.flush();
    }
  }

  /***
   * 表示当前的stripe已经构建完成，则做整理工作。
   * 1. 整理 stipe 级别的 statistics 信息
   * 2. 整理 file 级别的 statistics 信息
   * 3. 将 stipe 级别的 statistics 刷入到流
   * 4. 将 RowIndex 信息刷写到流
   * 5. 将 BloomFilter 信息刷写到流
   * @param requiredIndexEntries the number of index entries that are
   *                             required. this is to check to make sure the
   *                             row index is well formed.
   * @throws IOException
   */
  @Override
  public void writeStripe(int requiredIndexEntries) throws IOException {

    // 如果此列中没有空值，则 isPresent 流没有了意义，因为全为一
    if (isPresent != null && !foundNulls) {
      // 抑制输出
      isPresentOutStream.suppress();
      if (rowIndex != null) {
        removeIsPresentPositions();
      }
    }

    /* Update byte count */
    final long byteCount = context.getPhysicalWriter().getFileBytes(id, encryption);   // 获取列长度
    stripeColStatistics.updateByteCount(byteCount);                                    // 数据长度

    // 归并 file 级别的 statistics
    fileStatistics.merge(stripeColStatistics);

    // 将 stripe 级别的 statistics 刷写入流中
    context.writeStatistics(new StreamName(id, OrcProto.Stream.Kind.STRIPE_STATISTICS, encryption), stripeColStatistics.serialize());

    // 重置 stripe 统计信息
    stripeColStatistics.reset();

    foundNulls = false;

    context.setEncoding(id, encryption, getEncoding().build());

    // 如果存在 RowIndex, 则将当前的 RowIndex 刷写到流
    if (rowIndex != null) {
      if (rowIndex.getEntryCount() != requiredIndexEntries) {
        throw new IllegalArgumentException("Column has wrong number of " +
            "index entries found: " + rowIndex.getEntryCount() + " expected: " + requiredIndexEntries);
      }

      context.writeIndex(new StreamName(id, OrcProto.Stream.Kind.ROW_INDEX, encryption), rowIndex);
      rowIndex.clear();
      rowIndexEntry.clear();
    }

    // 将当前 stripe 列的Bloom过滤器信息刷写到流
    if (bloomFilterIndex != null) {
      context.writeBloomFilter(new StreamName(id, OrcProto.Stream.Kind.BLOOM_FILTER), bloomFilterIndex);
      bloomFilterIndex.clear();
    }
    // write the bloom filter to out stream
    if (bloomFilterIndexUtf8 != null) {
      context.writeBloomFilter(new StreamName(id, OrcProto.Stream.Kind.BLOOM_FILTER_UTF8), bloomFilterIndexUtf8);bloomFilterIndexUtf8.clear();
    }
  }

  /**
   * 获取列编码
   * @return the information about the encoding of this column
   */
  OrcProto.ColumnEncoding.Builder getEncoding() {

    OrcProto.ColumnEncoding.Builder builder =
        OrcProto.ColumnEncoding.newBuilder().setKind(OrcProto.ColumnEncoding.Kind.DIRECT);

    // 是否有 Bloom过滤器编码
    if (createBloomFilter) {
      builder.setBloomEncoding(BloomFilterIO.Encoding.CURRENT.getId());
    }

    return builder;
  }

  /**
   * 使用上一个位置和当前索引统计信息创建行索引条目。
   * 在清除索引统计信息之前，还将其合并到文件统计信息中。
   * 最后，它记录下一个索引的开始，并确保所有子列也创建一个条目。
   */
  @Override
  public void createRowIndexEntry() throws IOException {

    // 将指标内容合并到 stripe 级别的 statistics
    stripeColStatistics.merge(indexStatistics);

    // index 级别的 statistics 要合并到 RowIndexEntry 数据结构中
    rowIndexEntry.setStatistics(indexStatistics.serialize());

    // 行组统计信息置空用于下一次的数据写出
    indexStatistics.reset();

    //
    rowIndex.addEntry(rowIndexEntry);
    rowIndexEntry.clear();

    // 添加Bloom过滤器的相关信息
    addBloomFilterEntry();

    // rowIndexEntry 重新记录当前的偏移
    recordPosition(rowIndexPosition);
  }

  void addBloomFilterEntry() {
    if (createBloomFilter) {
      if (bloomFilter != null) {
        BloomFilterIO.serialize(bloomFilterEntry, bloomFilter);
        bloomFilterIndex.addBloomFilter(bloomFilterEntry.build());
        bloomFilter.reset();
      }
      if (bloomFilterUtf8 != null) {
        BloomFilterIO.serialize(bloomFilterEntry, bloomFilterUtf8);
        bloomFilterIndexUtf8.addBloomFilter(bloomFilterEntry.build());
        bloomFilterUtf8.reset();
      }
    }
  }

  @Override
  public void addStripeStatistics(StripeStatistics[] stats) throws IOException {
    // pick out the correct statistics for this writer
    int variantId;
    int relativeColumn;  // 列id
    if (encryption == null) {
      variantId = stats.length - 1;
      relativeColumn = id;
    } else {
      variantId = encryption.getVariantId();
      relativeColumn = id - encryption.getRoot().getId();
    }

    // 获取最后一个stripeStatitstics的当前列的列统计信息
    OrcProto.ColumnStatistics colStats = stats[variantId].getColumn(relativeColumn);
    // 更新到文件集的列统计信息
    fileStatistics.merge(ColumnStatisticsImpl.deserialize(schema, colStats));

    // 写入当前的 stripe_statistics
    context.writeStatistics(new StreamName(id, OrcProto.Stream.Kind.STRIPE_STATISTICS, encryption), colStats.toBuilder());
  }

  /**
   * 记录此列每个流中的当前位置。
   * @param recorder where should the locations be recorded
   */
  void recordPosition(PositionRecorder recorder) throws IOException {
    if (isPresent != null) {
      isPresent.getPosition(recorder);
    }
  }

  /**
   * Estimate how much memory the writer is consuming excluding the streams.
   * @return the number of bytes.
   */
  @Override
  public long estimateMemory() {
    long result = 0;
    if (isPresent != null) {
      result = isPresentOutStream.getBufferSize();
    }
    return result;
  }

  @Override
  public void writeFileStatistics() throws IOException {
    context.writeStatistics(
        new StreamName(id, OrcProto.Stream.Kind.FILE_STATISTICS, encryption),
        fileStatistics.serialize());
  }

  static class RowIndexPositionRecorder implements PositionRecorder {
    private final OrcProto.RowIndexEntry.Builder builder;

    RowIndexPositionRecorder(OrcProto.RowIndexEntry.Builder builder) {
      this.builder = builder;
    }

    @Override
    public void addPosition(long position) {
      builder.addPositions(position);
    }
  }

  @Override
  public void getCurrentStatistics(ColumnStatistics[] output) {
    output[id] = fileStatistics;
  }
}
