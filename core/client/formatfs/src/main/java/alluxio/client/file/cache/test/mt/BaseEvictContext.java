package alluxio.client.file.cache.test.mt;

import alluxio.client.file.cache.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class BaseEvictContext {
  public double mHitRatio;
  public long mHitSize;
  public long mVisitSize;
  protected double mCacheSize = 0;
  public long mTestFileLength;
  public long mCacheCapacity;
  public UnlockTask unlockTask = new UnlockTask();
  public ClientCacheContext mCacheContext;
  private long mLastVisitSize = 0;
  private long mLastHitSize = 0;
  private double mLastHRD;
  long mUserId;
  protected MTLRUEvictor mtlruEvictor;

  public double computePartialHitRatio() {
    if(mVisitSize == mLastVisitSize) {
      return 0;
    }
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
      access(unit, false);
    }
    return access(unit, false);
  }


  long access0(TmpCacheUnit unit, boolean isActual) {
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
    } else {
      mHitSize += unit.getSize();
    }
    if (isActual) {
      shareCount(unit, newSize > 0);
    }
    return newSize;
  }


  public void shareCount(TmpCacheUnit unit, boolean isNew) {
    if (isNew) {
      if (mtlruEvictor.mShareSet.containsKey(unit)) {
        mCacheSize -= unit.getSize();
        Set<Long> shareUser = mtlruEvictor.mShareSet.get(unit);
        if (!shareUser.contains(mUserId)) {
          shareUser.add(mUserId);
        }
        double reAddSize = (double) unit.getSize() / (double) shareUser.size();
        for (long id : shareUser) {
          mtlruEvictor.actualEvictContext.get(id).mCacheSize +=reAddSize;
        }
      } else {
        mtlruEvictor.mShareSet.put(unit, new HashSet<>());
        mtlruEvictor.mShareSet.get(unit).add(mUserId);
      }
    } else {
      Set<Long> shareUser = mtlruEvictor.mShareSet.get(unit);
      if (shareUser == null) {
        throw new RuntimeException("bug");
      } else {
        if (!shareUser.contains(mUserId)) {
          double reAddSize = (double) unit.getSize() / (double) shareUser.size();
          for (long id : shareUser) {
            mtlruEvictor.actualEvictContext.get(id).mCacheSize -=reAddSize;
          }
          shareUser.add(mUserId);
          reAddSize = (double) unit.getSize() / (double) shareUser.size();
          for (long id : shareUser) {
            mtlruEvictor.actualEvictContext.get(id).mCacheSize +=reAddSize;
          }
        }
      }
    }
  }


  public long cheatAccess0(TmpCacheUnit unit) {
    long newSize = 0;
    CacheUnit res1 = mCacheContext.getCache(unit.getFileId(), mTestFileLength, unit.getBegin(), unit.getEnd(), unlockTask);

    if (!res1.isFinish()) {
      TempCacheUnit unit1 = (TempCacheUnit)res1;
      mCacheContext.addCache(unit1);
      mCacheSize += res1.getSize();
      newSize += unit1.getSize();
    }
    shareCount(unit, newSize > 0);
    return newSize;
  }

  public void shareDelete(TmpCacheUnit deleteUnit) {
    double needDeleteSize = (double) deleteUnit.getSize() / (double) mtlruEvictor.mShareSet.get(deleteUnit).size();
    for (long id : mtlruEvictor.mShareSet.get(deleteUnit)) {
      mtlruEvictor.actualEvictContext.get(id).mCacheSize -= needDeleteSize;
    }
  }


  long remove0(TmpCacheUnit deleteUnit) {
    CacheUnit unit = mCacheContext.getCache(deleteUnit.getFileId(), mTestFileLength, deleteUnit.getBegin(),
            deleteUnit.getEnd(), unlockTask);
    long res = 0;
    if (unit.isFinish()) {
      mCacheContext.delete((CacheInternalUnit) unit);
      shareDelete(deleteUnit);
      res += unit.getSize();
    }
    return res;
  }


  public abstract List<TmpCacheUnit> getCacheList();

  public long access(TmpCacheUnit unit) {
    return access(unit, true);
  }

  public long access(TmpCacheUnit unit, boolean isActual) {
    fakeAccess(unit);
    return access0(unit, isActual);
  }

  public abstract void fakeAccess(TmpCacheUnit unit);

  public abstract long remove(TmpCacheUnit unit);

  public abstract TmpCacheUnit getEvictUnit();

  public abstract TmpCacheUnit getMaxPriorityUnit();

  public abstract void evict();

  public abstract void removeByShare(TmpCacheUnit deleteUnit);

  public long cheatAccess(TmpCacheUnit unit) {
    fakeAccess(unit);
    return cheatAccess0(unit);
  }
}
