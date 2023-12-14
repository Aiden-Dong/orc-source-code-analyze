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

/***
 * 作用：
 *  固定位长度写的优化类，为了减少在小于一个字节的位大写写出时的字节浪费。
 * 做法 : 将多个位压入一个字节中，复用同一个字节
 * 举例 :
 *    write : 0
 *    write : 1
 *    write : 1
 *    write : 0
 *
 *    ->
 *     bitsize : 1, bitLeft : 4, current :  0 1 1 0 0 0 0 0
 *    当消耗完字节空间时，将字节写出
 */
public class BitFieldWriter {
  private RunLengthByteWriter output;
  private final int bitSize; // 每次要写的位大小
  private byte current = 0;  // 当前写的字节数
  private int bitsLeft = 8;  // 当前值要写入的位数量

  public BitFieldWriter(PositionedOutputStream output, int bitSize) throws IOException {
    this.output = new RunLengthByteWriter(output);
    this.bitSize = bitSize;
  }

  private void writeByte() throws IOException {
    output.write(current);  //
    current = 0;
    bitsLeft = 8;
  }

  public void flush() throws IOException {
    if (bitsLeft != 8) {
      writeByte();
    }
    output.flush();
  }

  /****
   *
   * @param value
   * @throws IOException
   */
  public void write(int value) throws IOException {
    int bitsToWrite = bitSize;                           // 要写出的字节数

    while (bitsToWrite > bitsLeft) {
      // add the bits to the bottom of the current word
      current |= value >>> (bitsToWrite - bitsLeft);     // 高位插入 -- 大端字节流
      // subtract out the bits we just added
      bitsToWrite -= bitsLeft;                           // 降低一位
      // zero out the bits above bitsToWrite
      value &= (1 << bitsToWrite) - 1;                   // 高位置0
      writeByte();
    }
    bitsLeft -= bitsToWrite;
    current |= value << bitsLeft;                         // 同一个字节内部高位填充
    if (bitsLeft == 0) {
      writeByte();
    }
  }

  public void getPosition(PositionRecorder recorder) throws IOException {
    output.getPosition(recorder);
    recorder.addPosition(8 - bitsLeft);
  }

  public long estimateMemory() {
    return output.estimateMemory();
  }

  public void changeIv(Consumer<byte[]> modifier) {
    output.changeIv(modifier);
  }
}
