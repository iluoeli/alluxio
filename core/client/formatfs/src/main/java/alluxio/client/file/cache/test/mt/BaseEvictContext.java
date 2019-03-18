package alluxio.client.file.cache.test.mt;

import alluxio.client.file.cache.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class BaseEvictContext {
  public double mHitRatio;
  public long mHitSize;
  public long mVisitSize;
  protected long mCacheSize = 0;
  public long mTestFileLength;
  public long mCacheCapacity;
  public UnlockTask unlockTask = new UnlockTask();
  public ClientCacheContext mCacheContext;
  private long mLastVisitSize = 0;
  private long mLastHitSize = 0;
  private double mLastHRD;
  long mUserId;
  protected MTLRUEvictor mtlruEvictor;
  Set<TmpCacheUnit> mStoreSet = new HashSet<>();


  public void initHRD(double lastHRD) {
    mLastHRD = lastHRD;
  }

  public double getLastHRD() {
    return mLastHRD;
  }

  public double computePartialHitRatio() {
    return (double) (mHitSize - mLastHitSize) / (double) (mVisitSize - mLastVisitSize);
  }

  public void initAccessRecord() {
    mLastHitSize = mHitSize;
    mLastVisitSize = mVisitSize;
  }

  public BaseEvictContext(MTLRUEvictor test, ClientCacheContext cacheContext, long userId) {
    mTestFileLength = test.mTestFileLength;
    mCacheCapacity = test.cacheSize;
    mCacheContext = cacheContext;
    mUserId = userId;
    mtlruEvictor = test;
  }

  public BaseEvictContext resetCapacity(long capacitySize) {
    mCacheCapacity = capacitySize;
    return this;
  }

  public long accessByShare(TmpCacheUnit unit, ClientCacheContext sharedContext) {
    CacheUnit unit1 = sharedContext.getCache(unit.getFileId(), mTestFileLength, unit.getBegin(), unit.getEnd(), unlockTask);
    if (unit1.isFinish()) {
      access(unit);
    }
    return access(unit);
  }

  long access0(TmpCacheUnit unit) {
    long newSize = 0;
    mVisitSize += unit.getSize();
    CacheUnit res1 = mCacheContext.getCache(unit.getFileId(), mTestFileLength, unit.getBegin(), unit.getEnd(), unlockTask);
    if (!res1.isFinish()) {
      TempCacheUnit unit1 = (TempCacheUnit)res1;
      long missSize = mCacheContext.computeIncrese(unit1);
      mHitSize += (unit.getSize() - missSize);
      mCacheContext.addCache(unit1);
      newSize += missSize;
      mCacheSize += newSize;
      mStoreSet.add(unit);
    } else {
      mHitSize += unit.getSize();
    }

    return newSize;
  }

  long remove0(TmpCacheUnit deleteUnit) {
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
    return res;
  }


  public abstract List<TmpCacheUnit> getCacheList();

  public abstract long access(TmpCacheUnit unit);

  public abstract long remove(TmpCacheUnit unit);

  public abstract TmpCacheUnit getEvictUnit();

  public abstract TmpCacheUnit getMaxPriorityUnit();

  public abstract void evict();

  public abstract void removeByShare(TmpCacheUnit deleteUnit);
}
