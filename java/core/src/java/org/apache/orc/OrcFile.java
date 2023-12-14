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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.impl.HadoopShims;
import org.apache.orc.impl.HadoopShimsFactory;
import org.apache.orc.impl.KeyProvider;
import org.apache.orc.impl.MemoryManagerImpl;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.ReaderImpl;
import org.apache.orc.impl.WriterImpl;
import org.apache.orc.impl.WriterInternal;
import org.apache.orc.impl.writer.WriterImplV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Contains factory methods to read or write ORC files.
 * 读写 ORC 文件的接口
 */
public class OrcFile {

  private static final Logger LOG = LoggerFactory.getLogger(OrcFile.class);
  public static final String MAGIC = "ORC";

  /**
   * ORC 版本号：
   * 当前只支持 V_0_11, V_0_12
   *
   * 为ORC文件格式创建一个版本号，以便将来可以添加不兼容的更改。
   * 为了让用户更容易理解版本号，我们使用了最初编写该版本ORC文件的配置单元版本号。
   *
   * 因此，如果向ORC文件添加新编码或其他不兼容的更改，从而阻止旧读取器读取新格式，则应更改这些变量以反映下一个配置单元版本号。
   * 不应在补丁版本中添加不向前兼容的更改。
   *
   * 不要做任何破坏向后兼容性的更改，这将阻止新阅读器读取任何发布版本的Hive生成的ORC文件。
   */
  public enum Version {
    V_0_11("0.11", 0, 11),
    V_0_12("0.12", 0, 12),

    /**
     * 除测试外，请勿使用此格式。
     * 它将与软件的其他版本不兼容。当我们迭代ORC 2.0格式时，我们将在此版本下进行不兼容的格式更改，而不提供任何前向或后向兼容性。
     *
     * 2.0发布后，此版本标识符将完全删除。
     */
    UNSTABLE_PRE_2_0("UNSTABLE-PRE-2.0", 1, 9999),

    // 所有未知版本的通用标识符。
    FUTURE("future", Integer.MAX_VALUE, Integer.MAX_VALUE);

    // 当前版本
    public static final Version CURRENT = V_0_12;

    private final String name;  // 版本名
    private final int major;    // 大版本号
    private final int minor;    // 小版本号

    Version(String name, int major, int minor) {
      this.name = name;
      this.major = major;
      this.minor = minor;
    }

    public static Version byName(String name) {
      for(Version version: values()) {
        if (version.name.equals(name)) {
          return version;
        }
      }
      throw new IllegalArgumentException("Unknown ORC version " + name);
    }

    public String getName() {
      return name;
    }

    public int getMajor() {
      return major;
    }

    public int getMinor() {
      return minor;
    }
  }

  // ORC 写实现工具
  public enum WriterImplementation {
    ORC_JAVA(0), // ORC Java writer
    ORC_CPP(1),  // ORC C++ writer
    PRESTO(2),   // Presto writer
    SCRITCHLEY_GO(3), // Go writer from https://github.com/scritchley/orc
    TRINO(4),   // Trino writer
    UNKNOWN(Integer.MAX_VALUE);

    private final int id;

    WriterImplementation(int id) {
      this.id = id;
    }

    public int getId() {
      return id;
    }

    public static WriterImplementation from(int id) {
      WriterImplementation[] values = values();
      if (id >= 0 && id < values.length - 1) {
        return values[id];
      }
      return UNKNOWN;
    }
  }

  /**
   * 记录已修复错误的编写器版本。当您修复编写器中的错误（或进行实质性更改）而不更改文件格式时，
   * 请在此处添加新版本。
   *
   * 每个WriterImplementation从6开始依次分配ID，以便ORC-202之前的读取器正确对待其他写入器
   */
  public enum WriterVersion {
    // Java ORC Writer
    ORIGINAL(WriterImplementation.ORC_JAVA, 0),
    HIVE_8732(WriterImplementation.ORC_JAVA, 1),  // 修复了使用 utf8 表示最小/最大值的 条带/文件最大统计和字符串统计
    HIVE_4243(WriterImplementation.ORC_JAVA, 2),  // 使用Hive table 中的真实列名
    HIVE_12055(WriterImplementation.ORC_JAVA, 3), // vectorized writer
    HIVE_13083(WriterImplementation.ORC_JAVA, 4), // decimals write present stream correctly
    ORC_101(WriterImplementation.ORC_JAVA, 5),    // bloom filters use utf8
    ORC_135(WriterImplementation.ORC_JAVA, 6),    // timestamp stats use utc
    ORC_517(WriterImplementation.ORC_JAVA, 7),    // decimal64 min/max are fixed
    ORC_203(WriterImplementation.ORC_JAVA, 8),    // trim long strings & record they were trimmed
    ORC_14(WriterImplementation.ORC_JAVA, 9),     // column encryption added  -- 当前有效版本

    // C++ ORC Writer
    ORC_CPP_ORIGINAL(WriterImplementation.ORC_CPP, 6),

    // Presto Writer
    PRESTO_ORIGINAL(WriterImplementation.PRESTO, 6),

    // Scritchley Go Writer
    SCRITCHLEY_GO_ORIGINAL(WriterImplementation.SCRITCHLEY_GO, 6),

    // Trino Writer
    TRINO_ORIGINAL(WriterImplementation.TRINO, 6),

    // Don't use any magic numbers here except for the below:
    FUTURE(WriterImplementation.UNKNOWN, Integer.MAX_VALUE); // a version from a future writer

    private final int id;
    private final WriterImplementation writer;

    public WriterImplementation getWriterImplementation() {
      return writer;
    }

    public int getId() {
      return id;
    }

    WriterVersion(WriterImplementation writer, int id) {
      this.writer = writer;
      this.id = id;
    }

    private static final WriterVersion[][] values = new WriterVersion[WriterImplementation.values().length][];

    static {
      for(WriterVersion v: WriterVersion.values()) {
        WriterImplementation writer = v.writer;
        if (writer != WriterImplementation.UNKNOWN) {
          if (values[writer.id] == null) {
            values[writer.id] = new WriterVersion[WriterVersion.values().length];
          }
          if (values[writer.id][v.id] != null) {
            throw new IllegalArgumentException("Duplicate WriterVersion id " + v);
          }
          values[writer.id][v.id] = v;
        }
      }
    }

    public static WriterVersion from(WriterImplementation writer, int val) {
      if (writer == WriterImplementation.UNKNOWN) {
        return FUTURE;
      }
      if (writer != WriterImplementation.ORC_JAVA && val < 6) {
        throw new IllegalArgumentException("ORC File with illegal version " +
            val + " for writer " + writer);
      }
      WriterVersion[] versions = values[writer.id];
      if (val < 0 || versions.length <= val) {
        return FUTURE;
      }
      WriterVersion result = versions[val];
      return result == null ? FUTURE : result;
    }

    /**
     * Does this file include the given fix or come from a different writer?
     * @param fix the required fix
     * @return true if the required fix is present
     */
    public boolean includes(WriterVersion fix) {
      return writer != fix.writer || id >= fix.id;
    }
  }

  /**
   * The WriterVersion for this version of the software.
   */
  public static final WriterVersion CURRENT_WRITER = WriterVersion.ORC_14;

  // 编码策略
  public enum EncodingStrategy {
    SPEED, COMPRESSION
  }

  // 压缩策略
  public enum CompressionStrategy {
    SPEED, COMPRESSION
  }

  // unused
  protected OrcFile() {}

  // ORC 读相关配置
  public static class ReaderOptions {
    private final Configuration conf;
    private FileSystem filesystem;
    private long maxLength = Long.MAX_VALUE;
    private OrcTail orcTail;
    private KeyProvider keyProvider;
    // TODO: We can generalize FileMetadata interface. Make OrcTail implement FileMetadata interface
    // and remove this class altogether. Both footer caching and llap caching just needs OrcTail.
    // For now keeping this around to avoid complex surgery
    private FileMetadata fileMetadata;
    private boolean useUTCTimestamp;
    private boolean useProlepticGregorian;

    public ReaderOptions(Configuration conf) {
      this.conf = conf;
      this.useProlepticGregorian = OrcConf.PROLEPTIC_GREGORIAN.getBoolean(conf);
    }

    public ReaderOptions filesystem(FileSystem fs) {
      this.filesystem = fs;
      return this;
    }

    public ReaderOptions maxLength(long val) {
      maxLength = val;
      return this;
    }

    public ReaderOptions orcTail(OrcTail tail) {
      this.orcTail = tail;
      return this;
    }

    /**
     * Set the KeyProvider to override the default for getting keys.
     * @param provider
     * @return
     */
    public ReaderOptions setKeyProvider(KeyProvider provider) {
      this.keyProvider = provider;
      return this;
    }

    /**
     * Should the reader convert dates and times to the proleptic Gregorian
     * calendar?
     * @param newValue should it use the proleptic Gregorian calendar?
     * @return this
     */
    public ReaderOptions convertToProlepticGregorian(boolean newValue) {
      this.useProlepticGregorian = newValue;
      return this;
    }


    public Configuration getConfiguration() {
      return conf;
    }

    public FileSystem getFilesystem() {
      return filesystem;
    }

    public long getMaxLength() {
      return maxLength;
    }

    public OrcTail getOrcTail() {
      return orcTail;
    }

    public KeyProvider getKeyProvider() {
      return keyProvider;
    }

    /**
     * @deprecated Use {@link #orcTail(OrcTail)} instead.
     */
    public ReaderOptions fileMetadata(final FileMetadata metadata) {
      fileMetadata = metadata;
      return this;
    }

    public FileMetadata getFileMetadata() {
      return fileMetadata;
    }

    public ReaderOptions useUTCTimestamp(boolean value) {
      useUTCTimestamp = value;
      return this;
    }

    public boolean getUseUTCTimestamp() {
      return useUTCTimestamp;
    }

    public boolean getConvertToProlepticGregorian() {
      return useProlepticGregorian;
    }
  }

  public static ReaderOptions readerOptions(Configuration conf) {
    return new ReaderOptions(conf);
  }

  public static Reader createReader(Path path,
                                    ReaderOptions options) throws IOException {
    return new ReaderImpl(path, options);
  }

  public interface WriterContext {
    Writer getWriter();
  }

  public interface WriterCallback {
    void preStripeWrite(WriterContext context) throws IOException;
    void preFooterWrite(WriterContext context) throws IOException;
  }


  /***
   * BLOOM_FILTER 与 BLOOM_FILTER_UTF8 的区别是:
   *   -> BLOOM_FILTER 是将字符串序列化成默认字符集的字节流
   *   -> BLOOM_FILTER_UTF8 是将字符串序列化呈UTF8字符集对应的字节流
   */
  public enum BloomFilterVersion {
    // Include both the BLOOM_FILTER and BLOOM_FILTER_UTF8 streams to support  both old and new readers.
    // 包括BLOOM_FILTER和BLOOM_FILTER_UTF8流以支持新旧阅读器。
    // BLOOM_FIL
    ORIGINAL("original"),
    // Only include the BLOOM_FILTER_UTF8 streams that consistently use UTF8.
    // See ORC-101
    // 仅包括一致使用UTF8的BLOOM_FILTER_UTF8流。
    UTF8("utf8");

    private final String id;
    BloomFilterVersion(String id) {
      this.id = id;
    }

    @Override
    public String toString() {
      return id;
    }

    public static BloomFilterVersion fromString(String s) {
      for (BloomFilterVersion version: values()) {
        if (version.id.equals(s)) {
          return version;
        }
      }
      throw new IllegalArgumentException("Unknown BloomFilterVersion " + s);
    }
  }

  /**
   * Options for creating ORC file writers.
   */
  public static class WriterOptions implements Cloneable {
    private final Configuration configuration;
    private FileSystem fileSystemValue = null;
    private TypeDescription schema = null;
    private long stripeSizeValue;                       // stripe大小
    private long stripeRowCountValue;                   // stripe 行数量
    private long blockSizeValue;                        // block 大小
    private int rowIndexStrideValue;                    // ORC索引跨距
    private int bufferSizeValue;                        // buffer 大小
    private boolean enforceBufferSize = false;
    private boolean blockPaddingValue;                  // 是否应将stripe填充到HDFS块边界
    private CompressionKind compressValue;              // 压缩算法
    private MemoryManager memoryManagerValue;
    private Version versionValue;                       // ORC 版本
    private WriterCallback callback;
    private EncodingStrategy encodingStrategy;          // ORC 编码策略
    private CompressionStrategy compressionStrategy;    // ORC 压缩策略
    private double paddingTolerance;                    // 填充stripe的block最小剩余大小

    private String bloomFilterColumns;                    // Bloom过滤器对应列
    private double bloomFilterFpp;                       // Bloom 过滤器误报指数
    private BloomFilterVersion bloomFilterVersion;     // Bloom 过滤器版本

    private PhysicalWriter physicalWriter;
    private WriterVersion writerVersion = CURRENT_WRITER;   // 当前写的版本
    private boolean useUTCTimestamp;
    private boolean overwrite;                              // 是否写覆盖
    private boolean writeVariableLengthBlocks;              // 是否写入长度可变的Block
    private HadoopShims shims;                              // HDFS 写句柄
    private String directEncodingColumns;                   // 跳过字典编码列
    private String encryption;
    private String masks;
    private KeyProvider provider;
    private boolean useProlepticGregorian;
    private Map<String, HadoopShims.KeyMetadata> keyOverrides = new HashMap<>();

    protected WriterOptions(Properties tableProperties, Configuration conf) {
      configuration = conf;
      memoryManagerValue = getStaticMemoryManager(conf);                                          // MemoryManagerImpl

      overwrite = OrcConf.OVERWRITE_OUTPUT_FILE.getBoolean(tableProperties, conf);                 // 是否写覆盖
      stripeSizeValue = OrcConf.STRIPE_SIZE.getLong(tableProperties, conf);                        // stripe大小 -- 默认64M
      stripeRowCountValue = OrcConf.STRIPE_ROW_COUNT.getLong(tableProperties, conf);               // stripe 行数量 -- 默认INT_MAX
      blockSizeValue = OrcConf.BLOCK_SIZE.getLong(tableProperties, conf);                          // block 大小 -- 256M
      rowIndexStrideValue =  (int) OrcConf.ROW_INDEX_STRIDE.getLong(tableProperties, conf);        // ORC索引跨距
      bufferSizeValue = (int) OrcConf.BUFFER_SIZE.getLong(tableProperties, conf);                  // buffer 大小 -- 256KB
      blockPaddingValue = OrcConf.BLOCK_PADDING.getBoolean(tableProperties, conf);                 // 是否应将stripe填充到HDFS块边界
      compressValue = CompressionKind.valueOf(OrcConf.COMPRESS.getString(tableProperties, conf).toUpperCase());  // 流压缩算法

      enforceBufferSize = OrcConf.ENFORCE_COMPRESSION_BUFFER_SIZE.getBoolean(tableProperties, conf);

      String versionName = OrcConf.WRITE_FORMAT.getString(tableProperties, conf);                 // write 版本
      versionValue = Version.byName(versionName);

      String enString = OrcConf.ENCODING_STRATEGY.getString(tableProperties, conf);
      encodingStrategy = EncodingStrategy.valueOf(enString);

      String compString = OrcConf.COMPRESSION_STRATEGY.getString(tableProperties, conf);             // 定义压缩策略
      compressionStrategy = CompressionStrategy.valueOf(compString);

      paddingTolerance = OrcConf.BLOCK_PADDING_TOLERANCE.getDouble(tableProperties, conf);          // block 填充指数

      bloomFilterColumns = OrcConf.BLOOM_FILTER_COLUMNS.getString(tableProperties, conf);
      bloomFilterFpp = OrcConf.BLOOM_FILTER_FPP.getDouble(tableProperties, conf);
      bloomFilterVersion = BloomFilterVersion.fromString(OrcConf.BLOOM_FILTER_WRITE_VERSION.getString(tableProperties, conf));

      // org.apache.orc.impl.HadoopShimsPre2_7
      shims = HadoopShimsFactory.get();    // 获取 HDFS 操作句柄

      writeVariableLengthBlocks = OrcConf.WRITE_VARIABLE_LENGTH_BLOCKS.getBoolean(tableProperties,conf); // 是否写入长度可变的Block
      directEncodingColumns = OrcConf.DIRECT_ENCODING_COLUMNS.getString(tableProperties, conf);  // 跳过字典编码的列
      useProlepticGregorian = OrcConf.PROLEPTIC_GREGORIAN.getBoolean(conf);
    }

    /**
     * @return a SHALLOW clone
     */
    @Override
    public WriterOptions clone() {
      try {
        return (WriterOptions) super.clone();
      } catch (CloneNotSupportedException ex) {
        throw new AssertionError("Expected super.clone() to work");
      }
    }

    /**
     * Provide the filesystem for the path, if the client has it available.
     * If it is not provided, it will be found from the path.
     */
    public WriterOptions fileSystem(FileSystem value) {
      fileSystemValue = value;
      return this;
    }

    /**
     * If the output file already exists, should it be overwritten?
     * If it is not provided, write operation will fail if the file already exists.
     */
    public WriterOptions overwrite(boolean value) {
      overwrite = value;
      return this;
    }

    /**
     * Set the stripe size for the file. The writer stores the contents of the
     * stripe in memory until this memory limit is reached and the stripe
     * is flushed to the HDFS file and the next stripe started.
     */
    public WriterOptions stripeSize(long value) {
      stripeSizeValue = value;
      return this;
    }

    /**
     * Set the file system block size for the file. For optimal performance,
     * set the block size to be multiple factors of stripe size.
     */
    public WriterOptions blockSize(long value) {
      blockSizeValue = value;
      return this;
    }

    /**
     * Set the distance between entries in the row index. The minimum value is
     * 1000 to prevent the index from overwhelming the data. If the stride is
     * set to 0, no indexes will be included in the file.
     */
    public WriterOptions rowIndexStride(int value) {
      rowIndexStrideValue = value;
      return this;
    }

    /**
     * The size of the memory buffers used for compressing and storing the
     * stripe in memory. NOTE: ORC writer may choose to use smaller buffer
     * size based on stripe size and number of columns for efficient stripe
     * writing and memory utilization. To enforce writer to use the requested
     * buffer size use enforceBufferSize().
     */
    public WriterOptions bufferSize(int value) {
      bufferSizeValue = value;
      return this;
    }

    /**
     * Enforce writer to use requested buffer size instead of estimating
     * buffer size based on stripe size and number of columns.
     * See bufferSize() method for more info.
     * Default: false
     */
    public WriterOptions enforceBufferSize() {
      enforceBufferSize = true;
      return this;
    }

    /**
     * Sets whether the HDFS blocks are padded to prevent stripes from
     * straddling blocks. Padding improves locality and thus the speed of
     * reading, but costs space.
     */
    public WriterOptions blockPadding(boolean value) {
      blockPaddingValue = value;
      return this;
    }

    /**
     * Sets the encoding strategy that is used to encode the data.
     */
    public WriterOptions encodingStrategy(EncodingStrategy strategy) {
      encodingStrategy = strategy;
      return this;
    }

    /**
     * Sets the tolerance for block padding as a percentage of stripe size.
     */
    public WriterOptions paddingTolerance(double value) {
      paddingTolerance = value;
      return this;
    }

    /**
     * Comma separated values of column names for which bloom filter is to be created.
     */
    public WriterOptions bloomFilterColumns(String columns) {
      bloomFilterColumns = columns;
      return this;
    }

    /**
     * Specify the false positive probability for bloom filter.
     *
     * @param fpp - false positive probability
     * @return this
     */
    public WriterOptions bloomFilterFpp(double fpp) {
      bloomFilterFpp = fpp;
      return this;
    }

    /**
     * Sets the generic compression that is used to compress the data.
     */
    public WriterOptions compress(CompressionKind value) {
      compressValue = value;
      return this;
    }

    /**
     * Set the schema for the file. This is a required parameter.
     *
     * @param schema the schema for the file.
     * @return this
     */
    public WriterOptions setSchema(TypeDescription schema) {
      this.schema = schema;
      return this;
    }

    /**
     * Sets the version of the file that will be written.
     */
    public WriterOptions version(Version value) {
      versionValue = value;
      return this;
    }

    /**
     * Add a listener for when the stripe and file are about to be closed.
     *
     * @param callback the object to be called when the stripe is closed
     * @return this
     */
    public WriterOptions callback(WriterCallback callback) {
      this.callback = callback;
      return this;
    }

    /**
     * Set the version of the bloom filters to write.
     */
    public WriterOptions bloomFilterVersion(BloomFilterVersion version) {
      this.bloomFilterVersion = version;
      return this;
    }

    /**
     * Change the physical writer of the ORC file.
     * <p>
     * SHOULD ONLY BE USED BY LLAP.
     *
     * @param writer the writer to control the layout and persistence
     * @return this
     */
    public WriterOptions physicalWriter(PhysicalWriter writer) {
      this.physicalWriter = writer;
      return this;
    }

    /**
     * A public option to set the memory manager.
     */
    public WriterOptions memory(MemoryManager value) {
      memoryManagerValue = value;
      return this;
    }

    /**
     * Should the ORC file writer use HDFS variable length blocks, if they
     * are available?
     * @param value the new value
     * @return this
     */
    public WriterOptions writeVariableLengthBlocks(boolean value) {
      writeVariableLengthBlocks = value;
      return this;
    }

    /**
     * Set the HadoopShims to use.
     * This is only for testing.
     * @param value the new value
     * @return this
     */
    public WriterOptions setShims(HadoopShims value) {
      this.shims = value;
      return this;
    }

    /**
     * Manually set the writer version.
     * This is an internal API.
     *
     * @param version the version to write
     * @return this
     */
    protected WriterOptions writerVersion(WriterVersion version) {
      if (version == WriterVersion.FUTURE) {
        throw new IllegalArgumentException("Can't write a future version.");
      }
      this.writerVersion = version;
      return this;
    }

    /**
     * Manually set the time zone for the writer to utc.
     * If not defined, system time zone is assumed.
     */
    public WriterOptions useUTCTimestamp(boolean value) {
      useUTCTimestamp = value;
      return this;
    }

    /**
     * Set the comma-separated list of columns that should be direct encoded.
     * @param value the value to set
     * @return this
     */
    public WriterOptions directEncodingColumns(String value) {
      directEncodingColumns = value;
      return this;
    }

    /**
     * Encrypt a set of columns with a key.
     *
     * Format of the string is a key-list.
     * <ul>
     *   <li>key-list = key (';' key-list)?</li>
     *   <li>key = key-name ':' field-list</li>
     *   <li>field-list = field-name ( ',' field-list )?</li>
     *   <li>field-name = number | field-part ('.' field-name)?</li>
     *   <li>field-part = quoted string | simple name</li>
     * </ul>
     *
     * @param value a key-list of which columns to encrypt
     * @return this
     */
    public WriterOptions encrypt(String value) {
      encryption = value;
      return this;
    }

    /**
     * Set the masks for the unencrypted data.
     *
     * Format of the string is a mask-list.
     * <ul>
     *   <li>mask-list = mask (';' mask-list)?</li>
     *   <li>mask = mask-name (',' parameter)* ':' field-list</li>
     *   <li>field-list = field-name ( ',' field-list )?</li>
     *   <li>field-name = number | field-part ('.' field-name)?</li>
     *   <li>field-part = quoted string | simple name</li>
     * </ul>
     *
     * @param value a list of the masks and column names
     * @return this
     */
    public WriterOptions masks(String value) {
      masks = value;
      return this;
    }

    /**
     * For users that need to override the current version of a key, this
     * method allows them to define the version and algorithm for a given key.
     *
     * This will mostly be used for ORC file merging where the writer has to
     * use the same version of the key that the original files used.
     *
     * @param keyName the key name
     * @param version the version of the key to use
     * @param algorithm the algorithm for the given key version
     * @return this
     */
    public WriterOptions setKeyVersion(String keyName, int version,
                                       EncryptionAlgorithm algorithm) {
      HadoopShims.KeyMetadata meta = new HadoopShims.KeyMetadata(keyName,
          version, algorithm);
      keyOverrides.put(keyName, meta);
      return this;
    }

    /**
     * Set the key provider for column encryption.
     * @param provider the object that holds the master secrets
     * @return this
     */
    public WriterOptions setKeyProvider(KeyProvider provider) {
      this.provider = provider;
      return this;
    }

    /**
     * Should the writer use the proleptic Gregorian calendar for
     * times and dates.
     * @param newValue true if we should use the proleptic calendar
     * @return this
     */
    public WriterOptions setProlepticGregorian(boolean newValue) {
      this.useProlepticGregorian = newValue;
      return this;
    }

    public KeyProvider getKeyProvider() {
      return provider;
    }

    public boolean getBlockPadding() {
      return blockPaddingValue;
    }

    public long getBlockSize() {
      return blockSizeValue;
    }

    public String getBloomFilterColumns() {
      return bloomFilterColumns;
    }

    public boolean getOverwrite() {
      return overwrite;
    }

    public FileSystem getFileSystem() {
      return fileSystemValue;
    }

    public Configuration getConfiguration() {
      return configuration;
    }

    public TypeDescription getSchema() {
      return schema;
    }

    public long getStripeSize() {
      return stripeSizeValue;
    }

    public long getStripeRowCountValue() {
      return stripeRowCountValue;
    }

    public CompressionKind getCompress() {
      return compressValue;
    }

    public WriterCallback getCallback() {
      return callback;
    }

    public Version getVersion() {
      return versionValue;
    }

    public MemoryManager getMemoryManager() {
      return memoryManagerValue;
    }

    public int getBufferSize() {
      return bufferSizeValue;
    }

    public boolean isEnforceBufferSize() {
      return enforceBufferSize;
    }

    public int getRowIndexStride() {
      return rowIndexStrideValue;
    }

    public CompressionStrategy getCompressionStrategy() {
      return compressionStrategy;
    }

    public EncodingStrategy getEncodingStrategy() {
      return encodingStrategy;
    }

    public double getPaddingTolerance() {
      return paddingTolerance;
    }

    public double getBloomFilterFpp() {
      return bloomFilterFpp;
    }

    public BloomFilterVersion getBloomFilterVersion() {
      return bloomFilterVersion;
    }

    public PhysicalWriter getPhysicalWriter() {
      return physicalWriter;
    }

    public WriterVersion getWriterVersion() {
      return writerVersion;
    }

    public boolean getWriteVariableLengthBlocks() {
      return writeVariableLengthBlocks;
    }

    public HadoopShims getHadoopShims() {
      return shims;
    }

    public boolean getUseUTCTimestamp() {
      return useUTCTimestamp;
    }

    public String getDirectEncodingColumns() {
      return directEncodingColumns;
    }

    public String getEncryption() {
      return encryption;
    }

    public String getMasks() {
      return masks;
    }

    public Map<String, HadoopShims.KeyMetadata> getKeyOverrides() {
      return keyOverrides;
    }

    public boolean getProlepticGregorian() {
      return useProlepticGregorian;
    }
  }

  /**
   * Create a set of writer options based on a configuration.
   * @param conf the configuration to use for values
   * @return A WriterOptions object that can be modified
   */
  public static WriterOptions writerOptions(Configuration conf) {
    return new WriterOptions(null, conf);
  }

  /**
   * Create a set of write options based on a set of table properties and
   * configuration.
   * @param tableProperties the properties of the table
   * @param conf the configuration of the query
   * @return a WriterOptions object that can be modified
   */
  public static WriterOptions writerOptions(Properties tableProperties,
                                            Configuration conf) {
    return new WriterOptions(tableProperties, conf);
  }

  private static MemoryManager memoryManager = null;

  private static synchronized MemoryManager getStaticMemoryManager(Configuration conf) {
    if (memoryManager == null) {
      memoryManager = new MemoryManagerImpl(conf);
    }
    return memoryManager;
  }

  /**
   * 创建一个ORC文件写入器。这是创建将来写入器的公共接口，新的选项只会添加到这个方法。
   * @param path filename to write to
   * @param opts the options
   * @return a new ORC file writer
   * @throws IOException
   */
  public static Writer createWriter(Path path, WriterOptions opts) throws IOException {
    FileSystem fs = opts.getFileSystem() == null ? path.getFileSystem(opts.getConfiguration()) : opts.getFileSystem();

    switch (opts.getVersion()) {
      case V_0_11:
      case V_0_12:
        return new WriterImpl(fs, path, opts);
      case UNSTABLE_PRE_2_0:
        return new WriterImplV2(fs, path, opts);
      default:
        throw new IllegalArgumentException("Unknown version " + opts.getVersion());
    }
  }

  /**
   * Do we understand the version in the reader?
   * @param path the path of the file
   * @param reader the ORC file reader
   * @return is the version understood by this writer?
   */
  static boolean understandFormat(Path path, Reader reader) {
    if (reader.getFileVersion() == Version.FUTURE) {
      LOG.info("Can't merge {} because it has a future version.", path);
      return false;
    }
    if (reader.getWriterVersion() == WriterVersion.FUTURE) {
      LOG.info("Can't merge {} because it has a future writerVersion.", path);
      return false;
    }
    return true;
  }

  private static boolean sameKeys(EncryptionKey[] first,
                                  EncryptionKey[] next) {
    if (first.length != next.length) {
      return false;
    }
    for(int k = 0; k < first.length; ++k) {
      if (!first[k].getKeyName().equals(next[k].getKeyName()) ||
          first[k].getKeyVersion() != next[k].getKeyVersion() ||
          first[k].getAlgorithm() != next[k].getAlgorithm()) {
        return false;
      }
    }
    return true;
  }

  private static boolean sameMasks(DataMaskDescription[] first,
                                   DataMaskDescription[] next) {
    if (first.length != next.length) {
      return false;
    }
    for(int k = 0; k < first.length; ++k) {
      if (!first[k].getName().equals(next[k].getName())) {
        return false;
      }
      String[] firstParam = first[k].getParameters();
      String[] nextParam = next[k].getParameters();
      if (firstParam.length != nextParam.length) {
        return false;
      }
      for(int p=0; p < firstParam.length; ++p) {
        if (!firstParam[p].equals(nextParam[p])) {
          return false;
        }
      }
      TypeDescription[] firstRoots = first[k].getColumns();
      TypeDescription[] nextRoots = next[k].getColumns();
      if (firstRoots.length != nextRoots.length) {
        return false;
      }
      for(int r=0; r < firstRoots.length; ++r) {
        if (firstRoots[r].getId() != nextRoots[r].getId()) {
          return false;
        }
      }
    }
    return true;
  }

  private static boolean sameVariants(EncryptionVariant[] first,
                                      EncryptionVariant[] next) {
    if (first.length != next.length) {
      return false;
    }
    for(int k = 0; k < first.length; ++k) {
      if ((first[k].getKeyDescription() == null) !=
              (next[k].getKeyDescription() == null) ||
          !first[k].getKeyDescription().getKeyName().equals(
             next[k].getKeyDescription().getKeyName()) ||
          first[k].getRoot().getId() !=
             next[k].getRoot().getId()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Is the new reader compatible with the file that is being written?
   * @param firstReader the first reader that others must match
   * @param userMetadata the user metadata
   * @param path the new path name for warning messages
   * @param reader the new reader
   * @return is the reader compatible with the previous ones?
   */
  static boolean readerIsCompatible(Reader firstReader,
                                    Map<String, ByteBuffer> userMetadata,
                                    Path path,
                                    Reader reader) {
    // now we have to check compatibility
    TypeDescription schema = firstReader.getSchema();
    if (!reader.getSchema().equals(schema)) {
      LOG.info("Can't merge {} because of different schemas {} vs {}",
          path, reader.getSchema(), schema);
      return false;
    }
    CompressionKind compression = firstReader.getCompressionKind();
    if (reader.getCompressionKind() != compression) {
      LOG.info("Can't merge {} because of different compression {} vs {}",
          path, reader.getCompressionKind(), compression);
      return false;
    }
    OrcFile.Version fileVersion = firstReader.getFileVersion();
    if (reader.getFileVersion() != fileVersion) {
      LOG.info("Can't merge {} because of different file versions {} vs {}",
          path, reader.getFileVersion(), fileVersion);
      return false;
    }
    OrcFile.WriterVersion writerVersion = firstReader.getWriterVersion();
    if (reader.getWriterVersion() != writerVersion) {
      LOG.info("Can't merge {} because of different writer versions {} vs {}",
          path, reader.getFileVersion(), fileVersion);
      return false;
    }
    int rowIndexStride = firstReader.getRowIndexStride();
    if (reader.getRowIndexStride() != rowIndexStride) {
      LOG.info("Can't merge {} because of different row index strides {} vs {}",
          path, reader.getRowIndexStride(), rowIndexStride);
      return false;
    }
    for(String key: reader.getMetadataKeys()) {
      ByteBuffer currentValue = userMetadata.get(key);
      if (currentValue != null) {
        ByteBuffer newValue = reader.getMetadataValue(key);
        if (!newValue.equals(currentValue)) {
          LOG.info("Can't merge {} because of different user metadata {}", path,
              key);
          return false;
        }
      }
    }
    if (!sameKeys(firstReader.getColumnEncryptionKeys(),
                  reader.getColumnEncryptionKeys())) {
      LOG.info("Can't merge {} because it has different encryption keys", path);
      return false;
    }
    if (!sameMasks(firstReader.getDataMasks(), reader.getDataMasks())) {
      LOG.info("Can't merge {} because it has different encryption masks", path);
      return false;
    }
    if (!sameVariants(firstReader.getEncryptionVariants(),
                      reader.getEncryptionVariants())) {
      LOG.info("Can't merge {} because it has different encryption variants", path);
      return false;
    }
    if (firstReader.writerUsedProlepticGregorian() !=
            reader.writerUsedProlepticGregorian()) {
      LOG.info("Can't merge {} because it uses a different calendar", path);
      return false;
    }
    return true;
  }

  static void mergeMetadata(Map<String,ByteBuffer> metadata,
                            Reader reader) {
    for(String key: reader.getMetadataKeys()) {
      metadata.put(key, reader.getMetadataValue(key));
    }
  }

  /**
   * Merges multiple ORC files that all have the same schema to produce
   * a single ORC file.
   * The merge will reject files that aren't compatible with the merged file
   * so the output list may be shorter than the input list.
   * The stripes are copied as serialized byte buffers.
   * The user metadata are merged and files that disagree on the value
   * associated with a key will be rejected.
   *
   * @param outputPath the output file
   * @param options the options for writing with although the options related
   *                to the input files' encodings are overridden
   * @param inputFiles the list of files to merge
   * @return the list of files that were successfully merged
   * @throws IOException
   */
  public static List<Path> mergeFiles(Path outputPath,
                                      WriterOptions options,
                                      List<Path> inputFiles) throws IOException {
    Writer output = null;
    final Configuration conf = options.getConfiguration();
    KeyProvider keyProvider = options.getKeyProvider();
    try {
      byte[] buffer = new byte[0];
      Reader firstFile = null;
      List<Path> result = new ArrayList<>(inputFiles.size());
      Map<String, ByteBuffer> userMetadata = new HashMap<>();
      int bufferSize = 0;

      for (Path input : inputFiles) {
        FileSystem fs = input.getFileSystem(conf);
        Reader reader = createReader(input,
            readerOptions(options.getConfiguration())
                .filesystem(fs)
                .setKeyProvider(keyProvider));

        if (!understandFormat(input, reader)) {
          continue;
        } else if (firstFile == null) {
          // if this is the first file that we are including, grab the values
          firstFile = reader;
          bufferSize = reader.getCompressionSize();
          CompressionKind compression = reader.getCompressionKind();
          options.bufferSize(bufferSize)
              .version(reader.getFileVersion())
              .writerVersion(reader.getWriterVersion())
              .compress(compression)
              .rowIndexStride(reader.getRowIndexStride())
              .setSchema(reader.getSchema());
          if (compression != CompressionKind.NONE) {
            options.enforceBufferSize().bufferSize(bufferSize);
          }
          mergeMetadata(userMetadata, reader);
          // ensure that the merged file uses the same key versions
          for(EncryptionKey key: reader.getColumnEncryptionKeys()) {
            options.setKeyVersion(key.getKeyName(), key.getKeyVersion(),
                key.getAlgorithm());
          }
          output = createWriter(outputPath, options);
        } else if (!readerIsCompatible(firstFile, userMetadata, input, reader)) {
          continue;
        } else {
          mergeMetadata(userMetadata, reader);
          if (bufferSize < reader.getCompressionSize()) {
            bufferSize = reader.getCompressionSize();
            ((WriterInternal) output).increaseCompressionSize(bufferSize);
          }
        }
        EncryptionVariant[] variants = reader.getEncryptionVariants();
        List<StripeStatistics>[] completeList = new List[variants.length + 1];
        for(int v=0; v < variants.length; ++v) {
          completeList[v] = reader.getVariantStripeStatistics(variants[v]);
        }
        completeList[completeList.length - 1] = reader.getVariantStripeStatistics(null);
        StripeStatistics[] stripeStats = new StripeStatistics[completeList.length];
        try (FSDataInputStream inputStream = ((ReaderImpl) reader).takeFile()) {
          result.add(input);

          for (StripeInformation stripe : reader.getStripes()) {
            int length = (int) stripe.getLength();
            if (buffer.length < length) {
              buffer = new byte[length];
            }
            long offset = stripe.getOffset();
            inputStream.readFully(offset, buffer, 0, length);
            int stripeId = (int) stripe.getStripeId();
            for(int v=0; v < completeList.length; ++v) {
              stripeStats[v] = completeList[v].get(stripeId);
            }
            output.appendStripe(buffer, 0, length, stripe, stripeStats);
          }
        }
      }
      if (output != null) {
        for (Map.Entry<String, ByteBuffer> entry : userMetadata.entrySet()) {
          output.addUserMetadata(entry.getKey(), entry.getValue());
        }
        output.close();
      }
      return result;
    } catch (Throwable t) {
      if (output != null) {
        try {
          output.close();
        } catch (Throwable ignore) {
          // PASS
        }
        try {
          FileSystem fs = options.getFileSystem() == null ?
              outputPath.getFileSystem(conf) : options.getFileSystem();
          fs.delete(outputPath, false);
        } catch (Throwable ignore) {
          // PASS
        }
      }
      throw new IOException("Problem merging files into " + outputPath, t);
    }
  }
}
