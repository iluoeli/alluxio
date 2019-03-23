package alluxio.client.file.cache.test;

import alluxio.AlluxioURI;
import alluxio.client.file.CacheFileSystem;
import alluxio.client.file.FileSystem;
import alluxio.client.file.cache.*;
import alluxio.client.file.cache.remote.netty.CacheClient;
import alluxio.client.file.cache.remote.netty.CacheServer;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import io.netty.buffer.ByteBuf;

import java.util.List;

public class CacheClientServerTest extends testRead {
  private CacheServer server;
  private static ClientCacheContext mContext = ClientCacheContext.INSTANCE;
  private static long fileId;

  public static long readTime = 0;
  public static long beginTime = 0;
  public static void addCache() throws Exception {
    fileId = mContext.getMetedataCache().getStatus(new AlluxioURI("/testWriteBig"))
      .getFileId();

    CacheSet input = new CacheSet();

    for (int i = 0 ;i < 1024; i ++) {
      input.add(new BaseCacheUnit(fileId, i *1024 * 1024, i * 1024 * 1024+ 1024 * 1024));
      input.addSort(new BaseCacheUnit(fileId, i *1024 * 1024, i * 1024 *1024+ 1024 * 1024));
    }

    mContext.merge(input);
  }

  public static void startServer() throws Exception {
    CacheServer server = new CacheServer("0.0.0.0", 26666, mContext);
    server.launch();
  }

  public static void readTest() {
    byte [] b = new byte[1024 * 1024];
    long begin = System.currentTimeMillis();
    readTime = 0;
    for (int i = 0; i < 1024 ; i++) {
      List<ByteBuf> l = CacheClient.INSTANCE.read(fileId, 0,  1024 * 1024);
      CacheClient.INSTANCE.read0(l, b, 0, 10);
      for (ByteBuf ll : l) {
        ll.release();
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("time cost: "  + (end - begin));
    System.out.println("server cost " + readTime);
  }

  public static void main(String[] arg) throws Exception{
    //testRead.writeToAlluxio("/usr/local/test.gz", "/testWriteBig");

    FileSystem fs = CacheFileSystem.get(true);
    fs.openFile(new AlluxioURI("/testWriteBig"));
    addCache();
    System.out.println("add finish");
    startServer();
    System.out.println("start server");
    for (int i = 0 ; i <100; i ++)
    readTest();
    System.out.println("===============finish===============");
 //   System.out.print(1843920896 / (1024 * 1024));

   }
}
