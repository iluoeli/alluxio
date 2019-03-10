package alluxio.client.file.cache.test;

import alluxio.client.file.cache.BaseCacheUnit;
import alluxio.client.file.cache.CacheInternalUnit;
import alluxio.client.file.cache.ClientCacheContext;
import alluxio.client.file.cache.FileCacheUnit;
import alluxio.client.file.cache.submodularLib.stream.SieveStreamingHandler;
import alluxio.client.file.cache.submodularLib.stream.StreamHitHandler;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;

public class StreamTest {
  private SieveStreamingHandler mStreamHandler;
  private StreamHitHandler mHitHandler;
  private ClientCacheContext mCacheContext = ClientCacheContext.INSTANCE;

  public void init() {
    long fileLength = 1024 *1024 *1024;
    mStreamHandler = new SieveStreamingHandler(1000);
    mHitHandler = new StreamHitHandler();
    for(int i = 0 ; i < 1000; i ++) {
      Random r = new Random();
      int  begin = r.nextInt(1000);
      int length = r.nextInt(1000);
      int fileId = r.nextInt(1);
      mHitHandler.handle(new BaseCacheUnit(begin, begin+length, fileId),fileLength );

      mStreamHandler.handle(new BaseCacheUnit(begin, begin+length, fileId), fileLength);
    }
  }

  public void opt() {
    Map<Long, FileCacheUnit> m = mStreamHandler.getOPT();
    long res = 0;
    for(long l : m.keySet()) {
      FileCacheUnit unit = m.get(l);
      Iterator<CacheInternalUnit> iter  = unit.getCacheList().iterator();
      while(iter.hasNext()) {
        res += iter.next().getSize();
      }
    }
    System.out.println(res);
  }

  public static void main(String[] s) {
     StreamTest test = new StreamTest();
     test.init();
     test.opt();
  }
}
