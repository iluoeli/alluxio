package alluxio.client.file.cache.submodularLib.stream;

import alluxio.client.file.cache.core.BaseCacheUnit;
import alluxio.client.file.cache.core.CacheInternalUnit;
import alluxio.client.file.cache.core.ClientCacheContext;
import alluxio.client.file.cache.core.FileCacheUnit;
import alluxio.client.file.cache.struct.DoubleLinkedList;
import alluxio.client.file.cache.struct.LongPair;
import alluxio.exception.AlluxioException;

import java.io.IOException;
import java.util.*;

import static alluxio.client.file.cache.core.ClientCacheContext.mPromotionThreadId;

public class StreamPolicy implements Runnable {
  private SieveStreamingHandler mStreamHandler;
  private StreamHitHandler mHitHandler;
  private ClientCacheContext mCacheContext = ClientCacheContext.INSTANCE;

  public long update() throws IOException, AlluxioException {
    Map<Long, FileCacheUnit> resultCache = mStreamHandler.getOPT();
    long res = 0;
    for (long fileId : resultCache.keySet()) {
      FileCacheUnit fileUnit = resultCache.get(fileId);
      LinkedQueue queue = new LinkedQueue(fileUnit.getCacheList());
      FileCacheUnit unit = mCacheContext.mFileIdToInternalList.get(fileId);
      if (unit == null) {
        unit = new FileCacheUnit(fileId, mCacheContext.getMetedataCache().getStatus(fileId).getLength(), mCacheContext);
        mCacheContext.mFileIdToInternalList.put(fileId, unit);
      }

      res += unit.merge0(mCacheContext.getMetedataCache().getUri(fileId), queue);
    }
    return res;
  }

  public void filter(BaseCacheUnit unit1) {
    long fileLength = mCacheContext.getMetedataCache().getStatus(unit1.getFileId()).getLength();
    mHitHandler.handle(unit1, fileLength);
    mStreamHandler.handle(unit1, fileLength);
  }

  private boolean updateCheck() {
    return false;
  }

  public void init(long limit) {
    mStreamHandler = new SieveStreamingHandler(limit);
    mHitHandler = new StreamHitHandler();
  }

  @Override
  public void run() {
    mPromotionThreadId = Thread.currentThread().getId();
    System.out.println("streaming policy begins to run");
    while (true) {
      try {
        if (updateCheck()) {
          update();
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

  public class LinkedQueue extends LinkedList<LongPair> {
    private DoubleLinkedList<CacheInternalUnit> mCacheList;
    private CacheInternalUnit mCurrent = null;

    public LinkedQueue(DoubleLinkedList<CacheInternalUnit> list) {
      mCacheList = list;
      mCurrent = mCacheList.head.after;
    }

    public boolean isEmpty() {
      return mCurrent == null;
    }

    public LongPair poll() {
      LongPair res = new LongPair(mCurrent.getBegin(), mCurrent.getEnd());
      mCurrent = mCurrent.after;
      return res;
    }
  }

}
