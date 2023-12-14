package org.apache.orc.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.util.Map;
import java.util.Scanner;

public class TestMain {
  public static void main(String[] args) throws IOException {

    Configuration configuration = new Configuration();
    Path path = new Path("file:///Users/lan/work/haier/orc/tmp_file/part-00130-a2fd7b4f-8fd8-4a80-aa6e-9f1ebf44dbf0-c000.snappy.orc");

//     STEP 1 : 定义schema
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:string,z:boolean>");

    // STEP 2 : 创建 orc writer 对象
    OrcFile.WriterOptions options = OrcFile.writerOptions(configuration).setSchema(schema);
    Writer writer = OrcFile.createWriter(path, options);  // 内部构造 TreeWriter


    // STEP 3 : 声明RowBatch对象
    VectorizedRowBatch rowBatch = schema.createRowBatch();
    ColumnVector col1 = rowBatch.cols[0];


  }
}
