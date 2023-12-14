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

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

/**
 * ORC文件支持的配置选项
 */
public enum OrcConf {

  // 定义 orc stripe 的大小，单位 bytes
  // 默认 64MB
  STRIPE_SIZE("orc.stripe.size", "hive.exec.orc.default.stripe.size",
      64L * 1024 * 1024,"Define the default ORC stripe size, in bytes."),

  // 定义每行中可以包含的stripe的大小
  STRIPE_ROW_COUNT("orc.stripe.row.count", "orc.stripe.row.count",
      Integer.MAX_VALUE, "This value limit the row count in one stripe. \n" +
      "The number of stripe rows can be controlled at \n" +
      "(0, \"orc.stripe.row.count\" + max(batchSize, \"orc.rows.between.memory.checks\"))"),

  // 定义ORC文件的默认文件系统块大小
  // 默认 256MB
  BLOCK_SIZE("orc.block.size", "hive.exec.orc.default.block.size",
      256L * 1024 * 1024,
      "Define the default file system block size for ORC files."),

  // ORC writer 是否应创建索引作为文件的一部分。
  ENABLE_INDEXES("orc.create.index", "orc.create.index", true,
      "Should the ORC writer create indexes as part of the file."),

  // 以行数定义默认。（步幅是索引项表示的行数。）
  // 默认 10000
  ROW_INDEX_STRIDE("orc.row.index.stride",
      "hive.exec.orc.default.row.index.stride", 10000,
      "Define the default ORC index stride in number of rows. (Stride is the  number of rows an index entry represents.)"),

  // ORC BUFFUER 大小
  // 默认 256 KB
  BUFFER_SIZE("orc.compress.size", "hive.exec.orc.default.buffer.size",
      256 * 1024, "Define the default ORC buffer size, in bytes."),

  // 以 STRIPE_SIZE 和 BUFFER_SIZE 表示的 base write 和 delta write 的比率
  // 默认 8
  BASE_DELTA_RATIO("orc.base.delta.ratio", "hive.exec.orc.base.delta.ratio", 8,
      "The ratio of base writer and delta writer in terms of STRIPE_SIZE and BUFFER_SIZE."),

  // 是否应将stripe填充到HDFS块边界
  BLOCK_PADDING("orc.block.padding", "hive.exec.orc.default.block.padding",
      true,
      "Define whether stripes should be padded to the HDFS block boundaries."),

  // orc 默认的压缩算法
  // 默认 ZLIB
  COMPRESS("orc.compress", "hive.exec.orc.default.compress", "ZLIB",
      "Define the default compression codec for ORC file"),

  // ORC write 版本
  // 默认0.12
  WRITE_FORMAT("orc.write.format", "hive.exec.orc.write.format", "0.12",
      "Define the version of the file to write. Possible values are 0.11 and\n"+
          " 0.12. If this parameter is not defined, ORC will use the run\n" +
          " length encoding (RLE) introduced in Hive 0.12."),

  //
  ENFORCE_COMPRESSION_BUFFER_SIZE("orc.buffer.size.enforce",
      "hive.exec.orc.buffer.size.enforce", false,
      "Defines whether to enforce ORC compression buffer size."),

  // ORC 编码策略
  // 定义写入数据时要使用的编码策略。更改此选项只会影响整数的轻量级编码。
  // 此标志不会更改更高级别压缩编解码器（如ZLIB）的压缩级别。
  ENCODING_STRATEGY("orc.encoding.strategy", "hive.exec.orc.encoding.strategy",
      "SPEED",
      "Define the encoding strategy to use while writing data. Changing this\n"+
          "will only affect the light weight encoding for integers. This\n" +
          "flag will not change the compression level of higher level\n" +
          "compression codec (like ZLIB)."),

  // ORC 压缩策略
  // 定义写数据时的压测策略
  COMPRESSION_STRATEGY("orc.compression.strategy",
      "hive.exec.orc.compression.strategy", "SPEED",
      "Define the compression strategy to use while writing data.\n" +
          "This changes the compression level of higher level compression\n" +
          "codec (like ZLIB)."),
  /***
   * 将 block填充公差定义为stripe大小的小数（例如，默认值0.05为stripe size的5%）。
   * 对于 64Mb ORC stripe 和 256Mb HDFS block 的默认值，5% 的默认块填充容差将为256Mb块内的填充保留最大3.2Mb。
   * 在这种情况下，如果块中的可用大小大于3.2Mb，则将插入一个新的较小stripe以适应该空间。
   * 这将确保写入的stripe不会跨block边界并导致节点本地任务中的远程读取。
   */
  BLOCK_PADDING_TOLERANCE("orc.block.padding.tolerance",
      "hive.exec.orc.block.padding.tolerance", 0.05,
      "Define the tolerance for block padding as a decimal fraction of\n" +
          "stripe size (for example, the default value 0.05 is 5% of the\n" +
          "stripe size). For the defaults of 64Mb ORC stripe and 256Mb HDFS\n" +
          "blocks, the default block padding tolerance of 5% will\n" +
          "reserve a maximum of 3.2Mb for padding within the 256Mb block.\n" +
          "In that case, if the available size within the block is more than\n"+
          "3.2Mb, a new smaller stripe will be inserted to fit within that\n" +
          "space. This will make sure that no stripe written will block\n" +
          " boundaries and cause remote reads within a node local task."),

  // 定义布隆过滤器的默认误报概率
  // 0.05
  BLOOM_FILTER_FPP("orc.bloom.filter.fpp", "orc.default.bloom.fpp", 0.05,
      "Define the default false positive probability for bloom filters."),

  // 与ORC一起使用零拷贝读取。（这需要Hadoop 2.3或更高版本。） 默认不开启
  USE_ZEROCOPY("orc.use.zerocopy", "hive.exec.orc.zerocopy", false,
      "Use zerocopy reads with ORC. (This requires Hadoop 2.3 or later.)"),

  // 如果ORC读取器遇到损坏的数据，此值将用于确定是跳过损坏的数据还是引发异常。
  // 默认行为是抛出异常。
  SKIP_CORRUPT_DATA("orc.skip.corrupt.data", "hive.exec.orc.skip.corrupt.data",
      false,
      "If ORC reader encounters corrupt data, this value will be used to\n" +
          "determine whether to skip the corrupt data or throw exception.\n" +
          "The default behavior is to throw exception."),

  // 早于HIVE-4243的写入程序可能具有不准确的schema。
  //此设置将启用尽力而为的模式演变，而不是拒绝不匹配的模式
  TOLERATE_MISSING_SCHEMA("orc.tolerate.missing.schema",
      "hive.exec.orc.tolerate.missing.schema",
      true,
      "Writers earlier than HIVE-4243 may have inaccurate schema metadata.\n"
          + "This setting will enable best effort schema evolution rather\n"
          + "than rejecting mismatched schemas"),

  // orc write可以使用的堆的最大百分比
  MEMORY_POOL("orc.memory.pool", "hive.exec.orc.memory.pool", 0.5,
      "Maximum fraction of heap that can be used by ORC file writers"),

  // 如果字典中不同键的数量大于非空行总数的这一部分，请关闭字典编码。使用1始终使用字典编码。
  // 字典编码阈值
  // 默认 0.8
  DICTIONARY_KEY_SIZE_THRESHOLD("orc.dictionary.key.threshold",
      "hive.exec.orc.dictionary.key.size.threshold",
      0.8,
      "If the number of distinct keys in a dictionary is greater than this\n" +
          "fraction of the total number of non-null rows, turn off \n" +
          "dictionary encoding.  Use 1 to always use dictionary encoding."),

  // 如果启用，字典检查将发生在索引跨距的第一行之后（默认为10000行），
  // 否则字典检查将在写入第一条带之前发生。
  // 在这两种情况下，是否使用字典的决定将在此后保留。
  ROW_INDEX_STRIDE_DICTIONARY_CHECK("orc.dictionary.early.check",
      "hive.orc.row.index.stride.dictionary.check",
      true,
      "If enabled dictionary check will happen after first row index stride\n" +
          "(default 10000 rows) else dictionary check will happen before\n" +
          "writing first stripe. In both cases, the decision to use\n" +
          "dictionary or not will be retained thereafter."),

  // 字符串字典编码的具体实现类。
  // 默认 : 红黑树
  DICTIONARY_IMPL("orc.dictionary.implementation", "orc.dictionary.implementation",
      "rbtree",
      "the implementation for the dictionary used for string-type column encoding.\n" +
          "The choices are:\n"
          + " rbtree - use red-black tree as the implementation for the dictionary.\n"
          + " hash - use hash table as the implementation for the dictionary."),

  // 写入时要为其创建 BloomFile 的列集合
  BLOOM_FILTER_COLUMNS("orc.bloom.filter.columns", "orc.bloom.filter.columns",
      "", "List of columns to create bloom filters for when writing."),

  // BloomFilter 的版本
  BLOOM_FILTER_WRITE_VERSION("orc.bloom.filter.write.version",
      "orc.bloom.filter.write.version", OrcFile.BloomFilterVersion.UTF8.toString(),
      "Which version of the bloom filters should we write.\n" +
          "The choices are:\n" +
          "  original - writes two versions of the bloom filters for use by\n" +
          "             both old and new readers.\n" +
          "  utf8 - writes just the new bloom filters."),

  // reader 是否应该忽略过失的 non-utf8 BloomFilter
  IGNORE_NON_UTF8_BLOOM_FILTERS("orc.bloom.filter.ignore.non-utf8",
      "orc.bloom.filter.ignore.non-utf8", false,
      "Should the reader ignore the obsolete non-UTF8 bloom filters."),

  // 查找文件尾部时要读取的文件的最大大小。
  // 这主要用于流式摄取，以便在文件仍然打开时读取中间页脚
  MAX_FILE_LENGTH("orc.max.file.length", "orc.max.file.length", Long.MAX_VALUE,
      "The maximum size of the file to read for finding the file tail. This\n" +
          "is primarily used for streaming ingest to read intermediate\n" +
          "footers while the file is still open"),

  // 用户需要读取的 Schema : 使用 TypeDescription.fromString 解释这些值。
  MAPRED_INPUT_SCHEMA("orc.mapred.input.schema", null, null,
      "The schema that the user desires to read. The values are\n" +
      "interpreted using TypeDescription.fromString."),

  
  MAPRED_SHUFFLE_KEY_SCHEMA("orc.mapred.map.output.key.schema", null, null,
      "The schema of the MapReduce shuffle key. The values are\n" +
          "interpreted using TypeDescription.fromString."),

  MAPRED_SHUFFLE_VALUE_SCHEMA("orc.mapred.map.output.value.schema", null, null,
      "The schema of the MapReduce shuffle value. The values are\n" +
          "interpreted using TypeDescription.fromString."),

  MAPRED_OUTPUT_SCHEMA("orc.mapred.output.schema", null, null,
      "The schema that the user desires to write. The values are\n" +
          "interpreted using TypeDescription.fromString."),

  INCLUDE_COLUMNS("orc.include.columns", "hive.io.file.readcolumn.ids", null,
      "The list of comma separated column ids that should be read with 0\n" +
          "being the first column, 1 being the next, and so on. ."),

  KRYO_SARG("orc.kryo.sarg", "orc.kryo.sarg", null,
      "The kryo and base64 encoded SearchArgument for predicate pushdown."),
  KRYO_SARG_BUFFER("orc.kryo.sarg.buffer", null, 8192,
      "The kryo buffer size for SearchArgument for predicate pushdown."),

  SARG_COLUMNS("orc.sarg.column.names", "org.sarg.column.names", null,
      "The list of column names for the SearchArgument."),

  // 要求架构演化使用位置而不是列名来匹配顶级列。这提供了与Hive 2.1的向后兼容性。
  FORCE_POSITIONAL_EVOLUTION("orc.force.positional.evolution",
      "orc.force.positional.evolution", false,
      "Require schema evolution to match the top level columns using position\n" +
      "rather than column names. This provides backwards compatibility with\n" +
      "Hive 2.1."),

  FORCE_POSITIONAL_EVOLUTION_LEVEL("orc.force.positional.evolution.level",
      "orc.force.positional.evolution.level", 1,
      "Require schema evolution to match the the defined no. of level columns using position\n" +
          "rather than column names. This provides backwards compatibility with Hive 2.1."),

  // MemoryManager应多久检查一次内存大小？
  // 以添加到所有写入器的行为单位进行测量。有效范围为[110000]，主要用于测试。
  // 将此设置过低可能会对性能产生负面影响。使用 orc.stripe.row。如果值大于orc.stripe.row.count，则改为计数。
  ROWS_BETWEEN_CHECKS("orc.rows.between.memory.checks", "orc.rows.between.memory.checks", 5000,
    "How often should MemoryManager check the memory sizes? Measured in rows\n" +
      "added to all of the writers.  Valid range is [1,10000] and is primarily meant for" +
      "testing.  Setting this too low may negatively affect performance."
        + " Use orc.stripe.row.count instead if the value larger than orc.stripe.row.count."),

  // 如果文件已经存在则是否要覆盖
  // 默认不覆盖
  OVERWRITE_OUTPUT_FILE("orc.overwrite.output.file", "orc.overwrite.output.file", false,
    "A boolean flag to enable overwriting of the output file if it already exists.\n"),

  // schema 演化过程中是否区分大小写
  IS_SCHEMA_EVOLUTION_CASE_SENSITIVE("orc.schema.evolution.case.sensitive",
      "orc.schema.evolution.case.sensitive", true,
      "A boolean flag to determine if the comparision of field names " +
      "in schema evolution is case sensitive .\n"),

  // 用于确定是否允许SArg成为过滤器的布尔标志
  ALLOW_SARG_TO_FILTER("orc.sarg.to.filter", "org.sarg.to.filter", false,
                       "A boolean flag to determine if a SArg is allowed to become a filter"),

  // ？
  // 布尔标志，用于确定所选向量是否受读取应用程序支持。如果为false，则ORC读取器的输出必须重新应用筛选器，以避免在未选中的行中使用未设置的值。
  //如果不确定，请将其保留为false。。
  READER_USE_SELECTED("orc.filter.use.selected", "orc.filter.use.selected", false,
                        "A boolean flag to determine if the selected vector is supported by\n"
                        + "the reading application. If false, the output of the ORC reader "
                        + "must have the filter\n"
                        + "reapplied to avoid using unset values in the unselected rows.\n"
                        + "If unsure please leave this as false."),


  // 允许在读取期间使用插件筛选器。插件过滤器是针对服务 org.apache.orc.filter 发现的。
  // PluginFilterService，如果多个过滤器不是确定性的，并且过滤器功能不应依赖于应用程序的顺序。
  ALLOW_PLUGIN_FILTER("orc.filter.plugin",
                      "orc.filter.plugin",
                      false,
                      "Enables the use of plugin filters during read. The plugin filters "
                      + "are discovered against the service "
                      + "org.apache.orc.filter.PluginFilterService, if multiple filters are "
                      + "non-deterministic and the filter functionality should not depend on the "
                      + "order of application."),

  // 关于 ORC 写入器是否应写入可变长度 HDFS 块的布尔标志。
  WRITE_VARIABLE_LENGTH_BLOCKS("orc.write.variable.length.blocks", null, false,
      "A boolean flag as to whether the ORC writer should write variable length HDFS blocks."),

  // 要跳过字典编码的列
  DIRECT_ENCODING_COLUMNS("orc.column.encoding.direct", "orc.column.encoding.direct", "",
      "Comma-separated list of columns for which dictionary encoding is to be skipped."),


  // 要读取超过2GB的strip时，最大的限制块大小
  ORC_MAX_DISK_RANGE_CHUNK_LIMIT("orc.max.disk.range.chunk.limit",
      "hive.exec.orc.max.disk.range.chunk.limit",
    Integer.MAX_VALUE - 1024, "When reading stripes >2GB, specify max limit for the chunk size."),

  // 在确定连续读取时，此大小内的间隙将连续读取而不查找。
  // 默认值为零将禁用此优化
  ORC_MIN_DISK_SEEK_SIZE("orc.min.disk.seek.size",
                         "orc.min.disk.seek.size",
                         0,
                         "When determining contiguous reads, gaps within this size are "
                         + "read contiguously and not seeked. Default value of zero disables this optimization"),

  // 定义由于 orc.min.disk.seek.size 读取的额外字节的容差。
  // 如果（bytesRead-bytesNeeded）/ BytesReeded大于此阈值，则执行额外的工作以在读取之后从内存中删除额外的字节。
  ORC_MIN_DISK_SEEK_SIZE_TOLERANCE("orc.min.disk.seek.size.tolerance",
                          "orc.min.disk.seek.size.tolerance", 0.00,
                          "Define the tolerance for for extra bytes read as a result of orc.min.disk.seek.size. " +
                              "If the (bytesRead - bytesNeeded) / bytesNeeded is greater than this threshold then extra work is" +
                              "performed to drop the extra bytes from memory after the read."),

  // 要加密的密钥和列的列表
  ENCRYPTION("orc.encrypt", "orc.encrypt", null, "The list of keys and columns to encrypt with"),

  // 要应用于加密列的掩码
  DATA_MASK("orc.mask", "orc.mask", null, "The masks to apply to the encrypted columns"),

  // 用于加密的密钥提供程序的类型
  KEY_PROVIDER("orc.key.provider", "orc.key.provider", "hadoop",
      "The kind of KeyProvider to use for encryption."),

  PROLEPTIC_GREGORIAN("orc.proleptic.gregorian", "orc.proleptic.gregorian", false,
      "Should we read and write dates & times using the proleptic Gregorian calendar\n" +
          "instead of the hybrid Julian Gregorian? Hive before 3.1 and Spark before 3.0\n" +
          "used hybrid."),

  PROLEPTIC_GREGORIAN_DEFAULT("orc.proleptic.gregorian.default",
      "orc.proleptic.gregorian.default", false,
      "This value controls whether pre-ORC 27 files are using the hybrid or proleptic\n" +
      "calendar. Only Hive 3.1 and the C++ library wrote using the proleptic, so hybrid\n" +
      "is the default."),

  // ORC vector reader batch 中包含的行数。
  // 应仔细选择该值，以最小化开销并避免读取数据时出现OOM。
  ROW_BATCH_SIZE("orc.row.batch.size", "orc.row.batch.size", 1024,
      "The number of rows to include in a orc vectorized reader batch. " +
      "The value should be carefully chosen to minimize overhead and avoid OOMs in reading data."),

  // ORC行写入器将批写入文件之前要缓冲的最大子元素数
  ROW_BATCH_CHILD_LIMIT("orc.row.child.limit",
      "orc.row.child.limit",
      1024 * 32,
      "The maximum number of child elements to buffer before the ORC row writer writes the batch to the file.");

  private final String attribute;
  private final String hiveConfName;
  private final Object defaultValue;
  private final String description;

  OrcConf(String attribute,
          String hiveConfName,
          Object defaultValue,
          String description) {
    this.attribute = attribute;
    this.hiveConfName = hiveConfName;
    this.defaultValue = defaultValue;
    this.description = description;
  }

  public String getAttribute() {
    return attribute;
  }

  public String getHiveConfName() {
    return hiveConfName;
  }

  public Object getDefaultValue() {
    return defaultValue;
  }

  public String getDescription() {
    return description;
  }

  /***
   * 从 Properties 与 Configuration 中获取属性值
   * 优先选择从 properties 中获取，其次再从Configuration中查找
   *
   * @param tbl
   * @param conf
   * @return
   */
  private String lookupValue(Properties tbl, Configuration conf) {
    String result = null;
    if (tbl != null) {
      result = tbl.getProperty(attribute);
    }
    if (result == null && conf != null) {
      result = conf.get(attribute);
      if (result == null && hiveConfName != null) {
        result = conf.get(hiveConfName);
      }
    }
    return result;
  }

  public int getInt(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    if (value != null) {
      return Integer.parseInt(value);
    }
    return ((Number) defaultValue).intValue();
  }

  public int getInt(Configuration conf) {
    return getInt(null, conf);
  }

  /**
   * @deprecated Use {@link #getInt(Configuration)} instead. This method was
   * incorrectly added and shouldn't be used anymore.
   */
  @Deprecated
  public void getInt(Configuration conf, int value) {
    // noop
  }

  public void setInt(Configuration conf, int value) {
    conf.setInt(attribute, value);
  }

  public long getLong(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    if (value != null) {
      return Long.parseLong(value);
    }
    return ((Number) defaultValue).longValue();
  }

  public long getLong(Configuration conf) {
    return getLong(null, conf);
  }

  public void setLong(Configuration conf, long value) {
    conf.setLong(attribute, value);
  }

  public String getString(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    return value == null ? (String) defaultValue : value;
  }

  public String getString(Configuration conf) {
    return getString(null, conf);
  }

  public void setString(Configuration conf, String value) {
    conf.set(attribute, value);
  }

  public boolean getBoolean(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return (Boolean) defaultValue;
  }

  public boolean getBoolean(Configuration conf) {
    return getBoolean(null, conf);
  }

  public void setBoolean(Configuration conf, boolean value) {
    conf.setBoolean(attribute, value);
  }

  public double getDouble(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return ((Number) defaultValue).doubleValue();
  }

  public double getDouble(Configuration conf) {
    return getDouble(null, conf);
  }

  public void setDouble(Configuration conf, double value) {
    conf.setDouble(attribute, value);
  }
}
