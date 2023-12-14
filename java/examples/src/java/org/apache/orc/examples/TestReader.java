package org.apache.orc.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.*;
import org.apache.orc.impl.RecordReaderImpl;

import java.io.IOException;
import java.util.List;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : lan                                                                                *
 * @date : 2023/12/5                                                                                *
 *================================================================================================*/
public class TestReader {
  public static void main(String[] args) throws IOException {
    Path path = new Path("file:///Users/lan/work/haier/orc/tmp_file/part-00130-a2fd7b4f-8fd8-4a80-aa6e-9f1ebf44dbf0-c000.snappy.orc");
    Configuration configuration = new Configuration();

    // 创建 ReaderConfig
    OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(configuration);
    Reader reader = OrcFile.createReader(path, readerOptions);

    // 版本打印
    String writerId = reader.getWriterVersion().toString();
    String fileVersion = reader.getFileVersion().getName();
    String softwareVersion = reader.getSoftwareVersion();
    System.out.println("writer version : " + writerId);
    System.out.println("File version : " + fileVersion);
    System.out.println("software version : "  + softwareVersion);

    // 查看 postscript 内容
    CompressionKind compressionKind = reader.getCompressionKind();
    int metadataSize = reader.getMetadataSize();

    System.out.println("compression Kind : " + compressionKind);
    System.out.println("metadata size : " + metadataSize);

    // 查看footer 区内容
    long headerLength = reader.getFileTail().getFooter().getHeaderLength();
    long contentLength = reader.getFileTail().getFooter().getContentLength();
    String schema = reader.getSchema().toJson();


    long numberOfRows = reader.getFileTail().getFooter().getNumberOfRows();
    int rowIndexStride = reader.getFileTail().getFooter().getRowIndexStride();
    int stripesCount = reader.getFileTail().getFooter().getStripesCount();
    System.out.println("header : " + headerLength);
    System.out.println("content : " + contentLength);
    System.out.println(schema);
    System.out.println("numberOfRows : " + numberOfRows);
    System.out.println("rowIndexStride : " + rowIndexStride);
    System.out.println("stripesCount : " + stripesCount);

    for(int i = 0; i < stripesCount; i++){
      System.out.println("=================="+ i +"==============");
      OrcProto.StripeInformation stripes = reader.getFileTail().getFooter().getStripes(i);
      System.out.println(stripes);
      System.out.println("=======================================");
    }

    // 查看metadata 区
    List<StripeStatistics> stripeStatistics = reader.getStripeStatistics();
//    for (StripeStatistics stripeStatistic : stripeStatistics){
////      System.out.println("=======================================");
//      ColumnStatistics[] columnStatistics = stripeStatistic.getColumnStatistics();
//      for(ColumnStatistics columnStatic : columnStatistics){
////        System.out.println("--------------------------------------");
////        System.out.println(columnStatic);
////        System.out.println("--------------------------------------");
//      }
////      System.out.println("=======================================");
//    }

    ColumnStatistics[] columnStatistics = stripeStatistics.get(0).getColumnStatistics();
    for(int col = 0; col < columnStatistics.length ; col++){
//        System.out.println("--------------------------------------");
        System.out.println("col [" + col + "] : " + columnStatistics[col].toString().replaceAll("\n", ","));
//        System.out.println("--------------------------------------");
    }
    System.out.println("=========================================");

    // 获取 stripe 区域信息
    RecordReaderImpl rows = (RecordReaderImpl)reader.rows();
    OrcProto.StripeFooter stripeFooter = rows.readStripeFooter(reader.getStripes().get(0));

    // 获取列编码信息
    int columnsCount = stripeFooter.getColumnsCount();
    for (int col = 0; col < columnsCount; col++){
      System.out.println("col [" +col+"] :"+ stripeFooter.getColumns(col).toString().replace("\n", ","));
    }


    int streamsCount = stripeFooter.getStreamsCount();
    for (int i = 0; i < streamsCount ; i++){
      OrcProto.Stream streams = stripeFooter.getStreams(i);
      System.out.println(streams.toString().replace("\n", ","));
    }
    System.out.println("======================================");


    OrcProto.RowIndex rowGroupIndex = rows.readRowIndex(0, null, null).getRowGroupIndex()[1];
    int entryCount = rowGroupIndex.getEntryCount();
    for (OrcProto.RowIndexEntry rowIndexEntry : rowGroupIndex.getEntryList()) {
      System.out.println(rowIndexEntry.toString().replace("\n", ","));
    }


  }
}
