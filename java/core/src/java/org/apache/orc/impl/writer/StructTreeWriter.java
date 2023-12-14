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
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.util.List;

/***
 * StructTree 为集合类型,内部维护子节点的列集合
 */
public class StructTreeWriter extends TreeWriterBase {

  final TreeWriter[] childrenWriters;  // 子节点的字段集合

  public StructTreeWriter(TypeDescription schema,
                          WriterEncryptionVariant encryption,
                          WriterContext context) throws IOException {
    super(schema, encryption, context);
    List<TypeDescription> children = schema.getChildren();  // 获取子类型 schema

    // 初始化子类型 TreeWriter
    childrenWriters = new TreeWriter[children.size()];
    for (int i = 0; i < childrenWriters.length; ++i) {
      childrenWriters[i] = Factory.create(children.get(i), encryption, context);
    }
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);  // 记录 rowIndexPosition 当前偏移
    }
  }

  @Override
  public void writeRootBatch(VectorizedRowBatch batch, int offset, int length) throws IOException {

    // 记录此次写入的子类型的数量
    indexStatistics.increment(length);

    for (int i = 0; i < childrenWriters.length; ++i) {
      // 写每一个子列
      childrenWriters[i].writeBatch(batch.cols[i], offset, length);
    }
  }

  private static void writeFields(StructColumnVector vector,
                                  TreeWriter[] childrenWriters,
                                  int offset, int length) throws IOException {
    for (int field = 0; field < childrenWriters.length; ++field) {
      childrenWriters[field].writeBatch(vector.fields[field], offset, length);
    }
  }

  @Override
  public void writeBatch(ColumnVector vector,
                         int offset,
                         int length) throws IOException {

    // 记录基本信息
    super.writeBatch(vector, offset, length);
    StructColumnVector vec = (StructColumnVector) vector;

    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        writeFields(vec, childrenWriters, offset, length);
      }
    } else if (vector.noNulls) {
      writeFields(vec, childrenWriters, offset, length);
    } else {
      // write the records in runs
      int currentRun = 0;
      boolean started = false;
      for (int i = 0; i < length; ++i) {
        if (!vec.isNull[i + offset]) { // 非空
          if (!started) {
            started = true;
            currentRun = i;
          }
        } else if (started) {
          started = false;
          writeFields(vec, childrenWriters, offset + currentRun,
              i - currentRun);
        }
      }
      if (started) {
        writeFields(vec, childrenWriters, offset + currentRun, length - currentRun);
      }
    }
  }

  /***
   * 创建行组统计信息
   * @throws IOException
   */
  @Override
  public void createRowIndexEntry() throws IOException {
    super.createRowIndexEntry();
    for (TreeWriter child : childrenWriters) {
      child.createRowIndexEntry();
    }
  }

  @Override
  public void writeStripe(int requiredIndexEntries) throws IOException {
    super.writeStripe(requiredIndexEntries);

    for (TreeWriter child : childrenWriters) {
      child.writeStripe(requiredIndexEntries);
    }
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
  }

  @Override
  public void addStripeStatistics(StripeStatistics[] stats
                                  ) throws IOException {
    super.addStripeStatistics(stats);
    for (TreeWriter child : childrenWriters) {
      child.addStripeStatistics(stats);
    }
  }

  @Override
  public long estimateMemory() {
    long result = 0;
    for (TreeWriter writer : childrenWriters) {
      result += writer.estimateMemory();
    }
    return super.estimateMemory() + result;
  }

  @Override
  public long getRawDataSize() {
    long result = 0;
    for (TreeWriter writer : childrenWriters) {
      result += writer.getRawDataSize();
    }
    return result;
  }

  @Override
  public void writeFileStatistics() throws IOException {
    super.writeFileStatistics();
    for (TreeWriter child : childrenWriters) {
      child.writeFileStatistics();
    }
  }

  @Override
  public void flushStreams() throws IOException {
    super.flushStreams();
    for (TreeWriter child : childrenWriters) {
      child.flushStreams();
    }
  }

  @Override
  public void getCurrentStatistics(ColumnStatistics[] output) {
    super.getCurrentStatistics(output);
    for (TreeWriter child: childrenWriters) {
      child.getCurrentStatistics(output);
    }
  }

  @Override
  public void prepareStripe(int stripeId) {
    super.prepareStripe(stripeId);
    for (TreeWriter child: childrenWriters) {
      child.prepareStripe(stripeId);
    }
  }
}
