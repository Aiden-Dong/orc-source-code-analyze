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

import java.io.IOException;
import java.util.function.Consumer;

/**
 * RLE 编码
 * 一个用于写入字节序列的 streamFactory。
 * 在每个运行之前写入一个控制字节，正值 0 到 127 意味着 2 到 129 次重复。
 * 如果字节是 -1 到 -128，则后面跟着 1 到 128 个字面字节值。
 */
public class RunLengthByteWriter {
  static final int MIN_REPEAT_SIZE = 3;       // 达到重复的最低阈值
  static final int MAX_LITERAL_SIZE = 128;    // 当前写出的数据空间上限
  static final int MAX_REPEAT_SIZE= 127 + MIN_REPEAT_SIZE;     // 最大的重复数量
  private final PositionedOutputStream output;                 // 基于ByteBuffer的输出字节流
  private final byte[] literals = new byte[MAX_LITERAL_SIZE];  // 具体存放的 value 的内容
  private int numLiterals = 0;           // 当前 literals 中有多少个值
  private boolean repeat = false;        // 表示这个 byte 内容是否是重复内容
  private int tailRunLength = 0;         // 当前最后这个byte连续重复的情况

  public RunLengthByteWriter(PositionedOutputStream output) {
    this.output = output;
  }

  /***
   * 数据刷写到流
   * 如果是正值n，表示后续有一个有效值，这个有效值为重复度n的正值
   * 如果是负值-n, 表示后续有n个有效值
   * @throws IOException
   */
  private void writeValues() throws IOException {
    if (numLiterals != 0) {
      if (repeat) {
        output.write(numLiterals - MIN_REPEAT_SIZE);
        output.write(literals, 0, 1);
     } else {
        output.write(-numLiterals);
        output.write(literals, 0, numLiterals);
      }
      repeat = false;
      tailRunLength = 0;
      numLiterals = 0;
    }
  }

  public void flush() throws IOException {
    writeValues();
    output.flush();
  }

  /****
   * 将数据填充到 literals 中
   * 为了节省存储空间，在write中做了一个优化，并不是每个字节流都乖乖的写到 literals 中
   *    当单字节重复程度不高时则直接写入到 literals 中
   *    但是如果单字节重复程度较高， 则只记录重复的字节值跟重复数量标识
   * @param value
   * @throws IOException
   */
  public void write(byte value) throws IOException {
    if (numLiterals == 0) {
      // 如果literals中没有数据
      literals[numLiterals++] = value;
      tailRunLength = 1;
    } else if (repeat) {
      // 表示每次写入重复数据
      if (value == literals[0]) {
        numLiterals += 1;
        if (numLiterals == MAX_REPEAT_SIZE) {
          writeValues();
        }
      } else {
        writeValues();
        literals[numLiterals++] = value;
        tailRunLength = 1;
      }
    } else {
      if (value == literals[numLiterals - 1]) {
        // 表示value
        tailRunLength += 1;  // 重复度+1
      } else {
        tailRunLength = 1;   // 表示又写入一个新的
      }

      if (tailRunLength == MIN_REPEAT_SIZE) {
        // 如果达到了最小重复数量
        if (numLiterals + 1 == MIN_REPEAT_SIZE) {   // 表示从头到现在重复
          repeat = true;
          numLiterals += 1;
        } else {  // 表示之前又非重复数据需要先输出去
          // 刷写之前的数据
          numLiterals -= MIN_REPEAT_SIZE - 1;
          writeValues();
          // 设置重复数据
          literals[0] = value;
          repeat = true;
          numLiterals = MIN_REPEAT_SIZE;
        }
      } else {
        // 表示重复度不够，将 value 插入到
        literals[numLiterals++] = value;
        if (numLiterals == MAX_LITERAL_SIZE) {
          writeValues();
        }
      }
    }
  }

  /***
   * 标志当前的数据位置
   * @param recorder
   * @throws IOException
   */
  public void getPosition(PositionRecorder recorder) throws IOException {
    output.getPosition(recorder);
    recorder.addPosition(numLiterals);
  }

  public long estimateMemory() {
    return output.getBufferSize() + MAX_LITERAL_SIZE;
  }

  public void changeIv(Consumer<byte[]> modifier) {
    output.changeIv(modifier);
  }
}
