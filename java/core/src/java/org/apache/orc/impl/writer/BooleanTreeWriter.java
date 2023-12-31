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
import org.apache.orc.impl.BitFieldWriter;
import org.apache.orc.impl.CryptoUtils;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.PositionedOutputStream;
import org.apache.orc.impl.StreamName;

import java.io.IOException;

public class BooleanTreeWriter extends TreeWriterBase {

  private final BitFieldWriter writer;

  public BooleanTreeWriter(TypeDescription schema,
                           WriterEncryptionVariant encryption,
                           WriterContext context) throws IOException {
    super(schema, encryption, context);

    // 构建输出流
    PositionedOutputStream out = context.createStream(new StreamName(id, OrcProto.Stream.Kind.DATA, encryption));

    // 构建字节输出流
    this.writer = new BitFieldWriter(out, 1);

    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset, int length) throws IOException {
    // 基类的填充信息
    super.writeBatch(vector, offset, length);
    LongColumnVector vec = (LongColumnVector) vector;

    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        int value = vec.vector[0] == 0 ? 0 : 1;
        // 统计值为true的数量
        indexStatistics.updateBoolean(value != 0, length);
        for (int i = 0; i < length; ++i) {
          writer.write(value);
        }
      }
    } else {
      for (int i = 0; i < length; ++i) {
        if (vec.noNulls || !vec.isNull[i + offset]) {
          int value = vec.vector[i + offset] == 0 ? 0 : 1;
          writer.write(value);  //  将数据写给输出流
          indexStatistics.updateBoolean(value != 0, 1);
        }
      }
    }
  }

  @Override
  public void writeStripe(int requiredIndexEntries) throws IOException {
    super.writeStripe(requiredIndexEntries);
    if (rowIndexPosition != null) {
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
    long num = fileStatistics.getNumberOfValues();
    return num * JavaDataModel.get().primitive1();
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
