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
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.CryptoUtils;
import org.apache.orc.impl.IntegerWriter;
import org.apache.orc.impl.OutStream;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.StreamName;

import java.io.IOException;

public class IntegerTreeWriter extends TreeWriterBase {
  private final IntegerWriter writer;        // 数据流
  private boolean isDirectV2 = true;
  private final boolean isLong;

  public IntegerTreeWriter(TypeDescription schema,
                           WriterEncryptionVariant encryption,
                           WriterContext context) throws IOException {
    super(schema, encryption, context);

    // 当前数据Stream
    OutStream out = context.createStream(new StreamName(id, OrcProto.Stream.Kind.DATA, encryption));
    this.isDirectV2 = isNewWriteFormat(context);

    // Integer 数据写出器 {RunLengthIntegerWriter}
    this.writer = createIntegerWriter(out, true, isDirectV2, context);

    if (rowIndexPosition != null) {
      // 记录当前的偏移位置
      recordPosition(rowIndexPosition);
    }

    this.isLong = schema.getCategory() == TypeDescription.Category.LONG;
  }

  @Override
  OrcProto.ColumnEncoding.Builder getEncoding() {

    OrcProto.ColumnEncoding.Builder result = super.getEncoding();

    if (isDirectV2) {
      result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2);
    } else {
      result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT);
    }

    return result;
  }


  /***
   * 主要的工作内容如下:
   *  1. indexStatistics 统计最大值最小值
   *  2. indexStatistics 统计非空值的数量
   *  3. isPresent 记录非空判断位图向量
   *  4. write 记录有值数据体的具体内容
   *  5. bloomFileter 填充数据内容
   * @param vector 要写入的列向量内容
   * @param offset 要写入的列向量起始偏移
   * @param length 要写入的列向量长度内容
   * @throws IOException
   */
  @Override
  public void writeBatch(ColumnVector vector,
                         int offset,
                         int length) throws IOException {

    // 基类方法主要的工作是:
    //   1. 将非空值的统计数据写给 indexStatistics
    //   2. 将非空的位图向量写给 isPresent
    super.writeBatch(vector, offset, length);         // 处理非空问题

    LongColumnVector vec = (LongColumnVector) vector;

    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {

        long value = vec.vector[0];
        indexStatistics.updateInteger(value, length);
        if (createBloomFilter) {
          if (bloomFilter != null) {
            bloomFilter.addLong(value);
          }
          bloomFilterUtf8.addLong(value);
        }
        for (int i = 0; i < length; ++i) {
          // 写出到数据流
          writer.write(value);
        }
      }
    } else {
      for (int i = 0; i < length; ++i) {
        // 空值将直接跳过，因为有 isPresent 向量加持
        // 所以没有问题
        if (vec.noNulls || !vec.isNull[i + offset]) {
          long value = vec.vector[i + offset];
          writer.write(value);                                         // 写给输出流
          indexStatistics.updateInteger(value, 1);
          if (createBloomFilter) {
            if (bloomFilter != null) {
              bloomFilter.addLong(value);
            }
            bloomFilterUtf8.addLong(value);
          }
        }
      }
    }
  }

  @Override
  public void writeStripe(int requiredIndexEntries) throws IOException {
    super.writeStripe(requiredIndexEntries);
    if (rowIndexPosition != null) {
      // 记录当前的偏移位置
      recordPosition(rowIndexPosition);
    }
  }

  @Override
  void recordPosition(PositionRecorder recorder) throws IOException {
    super.recordPosition(recorder);
    writer.getPosition(recorder);
  }

  @Override
  public long estimateMemory() {
    return super.estimateMemory() + writer.estimateMemory();
  }

  @Override
  public long getRawDataSize() {
    JavaDataModel jdm = JavaDataModel.get();
    long num = fileStatistics.getNumberOfValues();
    return num * (isLong ? jdm.primitive2() : jdm.primitive1());
  }

  @Override
  public void flushStreams() throws IOException {
    super.flushStreams();
    writer.flush();
  }

  @Override
  public void prepareStripe(int stripeId) {
    super.prepareStripe(stripeId);
    writer.changeIv(CryptoUtils.modifyIvForStripe(stripeId));
  }
}
