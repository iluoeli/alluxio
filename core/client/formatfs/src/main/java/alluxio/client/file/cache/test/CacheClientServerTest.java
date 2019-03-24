package alluxio.client.file.cache.test;

import alluxio.AlluxioURI;
import alluxio.client.file.CacheFileSystem;
import alluxio.client.file.FileSystem;
import alluxio.client.file.cache.*;
import alluxio.client.file.cache.remote.FileCacheContext;
import alluxio.client.file.cache.remote.FileCacheEntity;
import alluxio.client.file.cache.remote.netty.CacheClient;
import alluxio.client.file.cache.remote.netty.CacheServer;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.ArrayList;
import java.util.List;

public class CacheClientServerTest extends testRead {
  private CacheServer server;
  private static FileCacheContext mContext = FileCacheContext.INSTANCE;
  private static long fileId;

  public static long readTime = 0;
  public static long beginTime = 0;

  public static void startServer() throws Exception {
    CacheServer server = new CacheServer("localhost", 8080, mContext);
    server.launch();
  }

  public static  void addCache() {
    long fileId =1;
    List<ByteBuf> tmp = new ArrayList<>();
    while(tmp.size() < 5) {
      tmp.add(ByteBufAllocator.DEFAULT.buffer(1024 * 1024 *2));
    }
    FileCacheEntity entity = new FileCacheEntity(0, 10 * 1024 * 1024, tmp);
    mContext.addCache(fileId, entity);
  }

  public static void readTest() throws Exception {
    CacheClient cacheClient = new CacheClient();
    cacheClient.testRead();
  }

  public static void main(String[] arg) throws Exception{
    addCache();
    System.out.println("add finish");
    startServer();
    System.out.println("start server");
    readTest();
    System.out.println("===============finish===============");
  }
}
