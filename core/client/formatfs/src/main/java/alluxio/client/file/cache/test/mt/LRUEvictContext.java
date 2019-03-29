package alluxio.client.file.cache.test.mt;

import alluxio.client.file.cache.CacheInternalUnit;
import alluxio.client.file.cache.CacheUnit;
import alluxio.client.file.cache.ClientCacheContext;
import alluxio.client.file.cache.TempCacheUnit;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class LRUEvictContext extends BaseEvictContext {
  LinkedList<TmpCacheUnit> mLRUList = new LinkedList<>();
  Set<TmpCacheUnit> accessSet = new HashSet<>();

  public LRUEvictContext(MTLRUEvictor test, ClientCacheContext cacheContext, long userId) {
    super(test, cacheContext, userId);
  }

  public void evict() {

    while (mCacheSize > mCacheCapacity ) {
      LinkedList<TmpCacheUnit> lruList = mLRUList;
      TmpCacheUnit deleteUnit = lruList.pollFirst();
      CacheUnit res = mCacheContext.getCache(deleteUnit.getFileId(), mTestFileLength, deleteUnit.getBegin(), deleteUnit.getEnd(), unlockTask);
      if (res.isFinish()) {
        mCacheSize -= res.getSize();
        mCacheContext.delete((CacheInternalUnit) res);
      }
      accessSet.remove(deleteUnit);
    }
  }

  public List<TmpCacheUnit> getCacheList() {
    return mLRUList;
  }

  public TmpCacheUnit getEvictUnit() {
    return mLRUList.peekFirst();
  }

  public void fakeAccess(TmpCacheUnit unit) {
    if (!accessSet.contains(unit)) {
      mLRUList.addLast(unit);
      accessSet.add(unit);
    } else {
      mLRUList.remove(unit);
      mLRUList.addLast(unit);
    }
  }

  public void fakeRemove(TmpCacheUnit deleteUnit){
    mLRUList.remove(deleteUnit);
    accessSet.remove(deleteUnit);
  }

  public void test () {
    long tmpSum = 0;
    for (TmpCacheUnit u : accessSet) {
      tmpSum += u.getSize();
    }
    if (tmpSum != mCacheSize) {
      throw new RuntimeException();
    }
  }

  @Override
  public void removeByShare(TmpCacheUnit deleteUnit) {
    if (accessSet.contains(deleteUnit)) {
      mLRUList.remove(deleteUnit);
      accessSet.remove(deleteUnit);
    }
  }

  public TmpCacheUnit getMaxPriorityUnit() {
    return mLRUList.peekLast();
  }

}

