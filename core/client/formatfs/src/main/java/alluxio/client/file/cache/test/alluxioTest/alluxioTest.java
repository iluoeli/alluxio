package alluxio.client.file.cache.test.alluxioTest;

import alluxio.AlluxioURI;
import alluxio.client.HitMetric;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.CacheFileSystem;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.cache.BaseCacheUnit;
import alluxio.client.file.cache.ClientCacheContext;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.OpenFileOptions;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.SerializationUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class alluxioTest {
  public static AlluxioURI writeToAlluxio(String s, String alluxioName) throws Exception {
    AlluxioURI uri = new AlluxioURI(alluxioName);
    FileSystem fs =FileSystem.Factory.get();
    if (fs.exists(uri)) {
      fs.delete(uri);
    }

    FileOutStream out = fs.createFile(uri, CreateFileOptions.defaults().setWriteType(WriteType.CACHE_THROUGH));
    File f = new File(s);
    BufferedInputStream in = new BufferedInputStream(new FileInputStream(f));
    byte[] b = new byte[1024 * 1024];
    int len = 0;
    long readLen = 0;
    while ((len = in.read(b)) > 0 && readLen < 1024 * 1024 * 512) {
      out.write(b, 0, len);
      readLen += len;
    }

    //in = new BufferedInputStream(new FileInputStream(f));
    //len = 0;
    //readLen = 0;
    //while ((len = in.read(b)) > 0 || readLen < 1024 * 1024 * 1024) {
    //  out.write(b, 0, len);
    //  readLen += len;
   // }

    out.close();
    return uri;
  }

  public static void testAlluxio() throws Exception {
    //long begin = System.currentTimeMillis();
    FileSystem fs = CacheFileSystem.get(true);

    System.out.println("read begin" + Thread.currentThread().getId());
    ClientCacheContext.checkout = 0;
    ClientCacheContext.missSize = 0;
    ClientCacheContext.hitTime = 0;
    Map<AlluxioURI, FileInStream> tmpIn = new HashMap<>();
    OpenFileOptions openFileOptions = OpenFileOptions.defaults().setReadType(ReadType
      .CACHE);
    for (int i = 0; i < 1024 * 2; i++) {
      int tmp = RandomUtils.nextInt(1, 5);
      AlluxioURI uri = new AlluxioURI("/" + tmp);

      FileInStream in;
      if (tmpIn.containsKey(uri)) {
        in = tmpIn.get(uri);
      } else {
        in = fs.openFile(uri, openFileOptions);
        tmpIn.put(uri, in);
      }

      int bufferLenth = RandomUtils.nextInt(1024 * 1024, 1024 * 1024 * 4);
      byte[] b = new byte[bufferLenth];
      long fileLength = fs.getStatus(uri).getLength();
      long beginMax = fileLength - bufferLenth;
      long readBegin = RandomUtils.nextLong(0, beginMax);

      in.positionedRead(readBegin, b, 0, bufferLenth);
      HitMetric.mAccessSize += bufferLenth;
      System.out.println("finish " + i);
      System.out.println(ClientCacheContext.INSTANCE.getAllSize(ClientCacheContext.INSTANCE) / (
        1024 * 1024));
    }

    long end = System.currentTimeMillis();
    // System.out.println();
  }

  public static void main(String [] args) throws Exception {
    //writeToAlluxio("/usr/local/test.gz", "/1");
    //writeToAlluxio("/usr/local/test.gz", "/2");
    //writeToAlluxio("/usr/local/test.gz", "/3");
    /*
    for(int i = 0 ;i < 10; i ++) {
     // writeToAlluxio("/usr/local/test.gz", "/bpc/" + i);
    }
  //  testAlluxio();
    FileSystem fs = FileSystem.Factory.get();
    FileInStream in = fs.openFile(new AlluxioURI("/bpc/1"), OpenFileOptions.defaults().setReadType
      (ReadType.CACHE));
    System.out.println(in.read());
    System.out.println("hit ratio " + (double) HitMetric.mMissSize / (double)(HitMetric.mAccessSize));
  */
    BaseCacheUnit unit = new BaseCacheUnit(1 ,1 ,1 );
    long tmp = System.currentTimeMillis();
    byte[] b = SerializationUtils.serialize(unit);
    System.out.println(System.currentTimeMillis() - tmp + " " + b.length);
  }
}
