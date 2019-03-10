package alluxio.client.file.cache.format;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

import alluxio.AlluxioURI;
import alluxio.proto.dataserver.Protocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetReader.Builder;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.*;

public class ParquetTest {

  static void parquetWriter(String outPath) throws Exception{

    alluxio.client.file.FileSystem fs = alluxio.client.file.FileSystem.Factory.get();
    if(fs.exists(new AlluxioURI("/test"))) {
      fs.delete(new AlluxioURI("/test"));
    }
    MessageType bigGroupSchema = Types.buildMessage().repeatedGroup().repeatedGroup().repeated
      (PrimitiveType.PrimitiveTypeName.INT32).
          named("ttl").named("smallGroup").named("midGroup").named("bigGroup");

    Path path = new Path(outPath);
    Configuration configuration = new Configuration();
    configuration.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem");
   // configuration.set("fs.AbstractFileSystem.alluxio.impl",
     // "alluxio.hadoop.AlluxioFileSystem");
    GroupWriteSupport writeSupport = new GroupWriteSupport();
    writeSupport.setSchema(bigGroupSchema, configuration);
    ParquetWriter<Group> writer = new ParquetWriter<Group>(path,configuration,writeSupport);


    System.out.println("write begin");
    MessageType smallGroupSchema = Types.buildMessage().repeated
      (PrimitiveType.PrimitiveTypeName.INT32).
        named("ttl").named("smallGroup");
    Group bigGroup =  new SimpleGroupFactory(bigGroupSchema).newGroup();
    Group smallGroup = new SimpleGroupFactory(smallGroupSchema).newGroup();


    for(int i = 0 ;i < 1024; i ++) {
      smallGroup.append("ttl",i);
    }
    System.out.println("small group init finish");
    MessageType midGroupSchema = Types.buildMessage().repeatedGroup().repeated(PrimitiveType.PrimitiveTypeName.INT32).
      named("ttl").named("smallGroup" ).named("midGroup");
    Group midGroup = new SimpleGroupFactory(midGroupSchema).newGroup();
    for (int j = 0 ; j < 1024; j ++) {
      midGroup.add("smallGroup", smallGroup);
    }
    for (int i = 0 ; i <1024; i ++) {
      bigGroup.add("midGroup", midGroup);
    }
    System.out.println("begin write");
    writer.write(bigGroup);
    writer.close();
    System.out.println("write finish");

  }

  static void parquetReaderV2(String inPath) throws Exception{
    GroupReadSupport readSupport = new GroupReadSupport();
    ParquetReader.Builder<Group> reader= ParquetReader.builder(readSupport, new Path(inPath));
    ParquetReader<Group> build=reader.build();
    Group line=null;

    ParquetMetadata readFooter = null;
    Path parquetFilePath = new Path(inPath);
    Configuration configuration = new Configuration(true);
    System.out.println("test0");
    readFooter = ParquetFileReader.readFooter(configuration, parquetFilePath, ParquetMetadataConverter.NO_FILTER);
    System.out.println("test1");
    MessageType schema =readFooter.getFileMetaData().getSchema();
    System.out.println("test2");
    line = build.read();
    for (int i = 0; i < 1024; i ++) {
      System.out.println("test" + i);

      line.getGroup("midGroup", i);
    }
    System.out.println("读取结束");
  }

  public static void main(String[] args) throws Exception {

    parquetReaderV2("alluxio://localhost:19998/test");

  }
}
