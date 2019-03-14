package alluxio.client.file.cache.test.MTTest;

import alluxio.client.file.cache.CacheInternalUnit;
import alluxio.client.file.cache.CacheUnit;
import alluxio.client.file.cache.ClientCacheContext;
import alluxio.client.file.cache.TempCacheUnit;
import alluxio.client.file.cache.test.LRUEvictor;
import com.google.common.base.Preconditions;

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
      CacheUnit res = mCacheContext.getCache(mTestFileId, mTestFileLength, deleteUnit.getBegin(), deleteUnit.getEnd(), unlockTask);
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


  public long access(TmpCacheUnit unit) {
    long newSize = 0;
    CacheUnit res = mCacheContext.getCache(mTestFileId, mTestFileLength, unit.getBegin(), unit.getEnd(), unlockTask);
    mVisitSize += unit.getSize();
    if (!accessSet.contains(unit)) {
      mLRUList.addLast(unit);
      accessSet.add(unit);
    } else {
      mLRUList.remove(unit);
      mLRUList.addLast(unit);
    }
    if (!res.isFinish()) {
      TempCacheUnit unit1 = (TempCacheUnit)res;
      long missSize = mCacheContext.computeIncrese(unit1);
      mHitSize += (unit.getSize() - missSize);
      mCacheContext.addCache(unit1);
      newSize += missSize;
      mCacheSize += newSize;
      mStoreSet.add(unit);
    } else {
      mHitSize += unit.getSize();
    }
    //test();
    return newSize;
  }
  public long remove(TmpCacheUnit deleteUnit) {
    CacheUnit unit = mCacheContext.getCache(deleteUnit.getFileId(), mTestFileLength, deleteUnit.getBegin(),
      deleteUnit.getEnd(), unlockTask);
    long res = 0;
    if (unit.isFinish()) {
      mCacheContext.delete((CacheInternalUnit) unit);
      if (mStoreSet.contains(deleteUnit)) {
        mStoreSet.remove(deleteUnit);
        mCacheSize -= deleteUnit.getSize();
      }
      res += unit.getSize();
    }
    if (accessSet.contains(deleteUnit)) {
      mLRUList.remove(deleteUnit);
      accessSet.remove(deleteUnit);
    }
    return res;
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

  public void removeByShare(TmpCacheUnit deleteUnit) {
    if (accessSet.contains(deleteUnit)) {
      mLRUList.remove(deleteUnit);
      accessSet.remove(deleteUnit);
      if (mStoreSet.contains(deleteUnit)) {
        mStoreSet.remove(deleteUnit);
        mCacheSize -= deleteUnit.getSize();
      }
    }
  }

  public TmpCacheUnit getMaxPriorityUnit() {
    return mLRUList.peekLast();
  }

}

