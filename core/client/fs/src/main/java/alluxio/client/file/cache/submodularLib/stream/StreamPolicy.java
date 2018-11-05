package alluxio.client.file.cache.submodularLib.stream;

import alluxio.client.file.cache.*;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import alluxio.exception.AlluxioException;

import javax.xml.crypto.dsig.XMLSignature;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static alluxio.client.file.cache.ClientCacheContext.mPromotionThreadId;

public class StreamPolicy implements Runnable {
  private SieveStreaming mStream;
  private StreamHitHandler mHitHandler;
  private ClientCacheContext mCacheContext = ClientCacheContext.INSTANCE;

  public long updateCache() throws IOException, AlluxioException {
    Map<Long, FileCacheUnit> resultCache = mStream.getOPT();
    long res = 0;
    for (long fileId : resultCache.keySet()) {
      FileCacheUnit fileUnit = resultCache.get(fileId);
      LinkedQueue queue = new LinkedQueue(fileUnit.getCacheList());
      FileCacheUnit unit = mCacheContext.mFileIdToInternalList.get(fileId);
      if (unit == null) {
        unit = new FileCacheUnit(fileId, mCacheContext.metedataCache.getStatus(fileId).getLength(), mCacheContext.getLockManager());
        mCacheContext.mFileIdToInternalList.put(fileId, unit);
      }

      res += unit.merge(mCacheContext.metedataCache.getUri(fileId), queue);
    }
    return res;
  }

  public CacheSet move(CacheSet s1, CacheSet s2) {
    for (long fileId : s2.cacheMap.keySet()) {
      Set<CacheUnit> s = s2.get(fileId);
      if (!s1.cacheMap.containsKey(fileId)) {
        s1.cacheMap.put(fileId, new TreeSet<>(new Comparator<CacheUnit>() {
          @Override
          public int compare(CacheUnit o1, CacheUnit o2) {
            return 0;
          }
        }));
      }
      s1.get(fileId).addAll(s);
    }
    s2.clear();
    return s1;
  }

  public void filter(BaseCacheUnit unit1) {
    long fileLength = mCacheContext.metedataCache.getStatus(unit1.getFileId()).getLength();

  }

  public void promote() {


  }


  private boolean promoteCheck() {
  }

  public void init(long limit) {
    mStream = new SieveStreaming(limit);
    mHitHandler = new StreamHitHandler();
  }

  @Override
  public void run() {
    mPromotionThreadId = Thread.currentThread().getId();
    System.out.println("promoter begins to run");
    while (true) {
      try {
        if (promoteCheck()) {
          promote();
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

}
