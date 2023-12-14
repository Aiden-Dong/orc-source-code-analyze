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

package org.apache.orc;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * A memory manager that keeps a global context of how many ORC writers there are and manages the memory between them.
 * 一个内存管理器，用于保存有多少ORC写入器的全局上下文，并管理它们之间的内存。
 * For use cases with dynamic partitions, it is easy to end up with many writers in the same task.
 * 对于具有动态分区的用例，很容易在同一任务中出现许多编写器。
 * By managing the size of each allocation, we try to cut down the size of each allocation and keep the task from running out of memory.
 * 通过管理每个分配的大小，我们试图减少每个分配的规模，并防止任务内存不足。
 *
 * This class is not thread safe,
 * 该类不是线程安全的，
 * but is re-entrant - ensure creation and all invocations are triggered from the same thread.
 * 但是是可重入的-确保创建和所有调用都是从同一线程触发的。
 */
public interface MemoryManager {

  interface Callback {
    /**
     * 当stripe大小被改变时， 应当适当调整内存大小来满足新的写请求
     * The scale factor for the stripe size has changed and thus the writer should adjust their desired size appropriately.
     * @param newScale the current scale factor for memory allocations 当前的内存分配因子
     * @return true if the writer was over the limit
     */
    boolean checkMemory(double newScale) throws IOException;
  }

  /**
   * 将新写入程序的内存分配添加到池中。我们使用路径作为唯一的密钥，以确保不会得到重复。
   * 每一个路径作为一个写请求
   * Add a new writer's memory allocation to the pool. We use the path as a unique key to ensure that we don't get duplicates.
   * @param path the file that is being written
   * @param requestedAllocation the requested buffer size
   */
  void addWriter(Path path, long requestedAllocation, Callback callback) throws IOException;

  /**
   * Remove the given writer from the pool.
   * 从内存管理池中移除一个写
   * @param path the file that has been closed
   */
  void removeWriter(Path path) throws IOException;

  /**
   * 给内存管理器一个执行内存检查的机会
   * Give the memory manager an opportunity for doing a memory check.
   * @param rows number of rows added
   * @throws IOException
   * @deprecated Use {@link MemoryManager#checkMemory} instead
   */
  void addedRow(int rows) throws IOException;

  /**
   * As part of adding rows, the writer calls this method to determine
   * if the scale factor has changed. If it has changed, the Callback will be
   * called.
   * @param previousAllocation the previous allocation
   * @param writer the callback to call back into if we need to
   * @return the current allocation
   */
  default long checkMemory(long previousAllocation,
                           Callback writer) throws IOException {
    addedRow(1024);
    return previousAllocation;
  }
}
