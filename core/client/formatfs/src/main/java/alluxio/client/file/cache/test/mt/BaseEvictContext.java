package alluxio.client.file.cache.test.mt;

import alluxio.client.file.cache.*;
import com.google.common.base.Preconditions;

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
  public static Set<TmpCacheUnit> testSet = new HashSet<>();

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

  public long accessByShare(TmpCacheUnit unit,ClientCacheContext context) {
     Set<Long> shareSet = mtlruEvictor.mShareSet.get(unit);
     long newSize = access(unit, false);
     if (shareSet != null && newSize > 0) {
       if (!shareSet.contains(mUserId) || shareSet.size() > 1) {

         long shareNumber = shareSet.contains(mUserId) ? shareSet.size() : shareSet.size() + 1;
         mHitSize += (double) unit.getSize() / shareNumber;
         double otherUserPart = ((double) unit.getSize() / (double) shareNumber) * (shareNumber - 1);
         mCacheSize -= otherUserPart;
         mVisitSize -= otherUserPart;
        // mHitSize += (double) unit.getSize();
         //mCacheSize -= otherUserPart;
        // mVisitSize -= otherUserPart;
       }
     }
     return newSize;
  }

  /*
  public long accessByShare(TmpCacheUnit unit, ClientCacheContext sharedContext) {
    CacheUnit unit1 = sharedContext.getCache(unit.getFileId(), mTestFileLength, unit.getBegin(), unit.getEnd(), unlockTask);
    if (unit1.isFinish()) {
      access(unit);
    }
    return access(unit);

  }*/

  long access0(TmpCacheUnit unit, boolean isActual) {
    long newSize = 0;
    mVisitSize += unit.getSize();
    CacheUnit res1 = mCacheContext.getCache(unit.getFileId(), mTestFileLength, unit.getBegin(), unit.getEnd(), unlockTask);
    if (!res1.isFinish()) {
      TempCacheUnit unit1 = (TempCacheUnit)res1;
      mCacheContext.addCache(unit1);
      newSize = unit.getSize();
      mCacheSize += newSize;
    } else {
      mHitSize += unit.getSize();
    }
    if (isActual) {
     // ((LFUEvictContext)this).check();
      shareCount(unit, newSize > 0);
     // ((LFUEvictContext)this).check();
    }
    return newSize;
  }


  public void shareCount(TmpCacheUnit unit, boolean isNew) {
    if (isNew) {
      if (mtlruEvictor.mShareSet.containsKey(unit)) {
        throw new RuntimeException("bug");
      } else {
        mtlruEvictor.mShareSet.put(unit, new HashSet<>());
        mtlruEvictor.mShareSet.get(unit).add(mUserId);
        testSet.add(unit);
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
            //((LFUEvictContext)mtlruEvictor.actualEvictContext.get(id)).check();
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
    Set<Long> tmp = new HashSet<>(mtlruEvictor.mShareSet.get(deleteUnit));

    mtlruEvictor.mShareSet.remove(deleteUnit);
    for (long id : tmp) {
      mtlruEvictor.actualEvictContext.get(id).mCacheSize -= needDeleteSize;
      mtlruEvictor.actualEvictContext.get(id).fakeRemove(deleteUnit);
    }
  }


  private long remove0(TmpCacheUnit deleteUnit, boolean isActual) {
    CacheUnit unit = mCacheContext.getCache(deleteUnit.getFileId(), mTestFileLength, deleteUnit.getBegin(),
            deleteUnit.getEnd(), unlockTask);
    long res = 0;
    if (unit.isFinish()) {
      mCacheContext.delete((CacheInternalUnit) unit);
      if (isActual) {
        shareDelete(deleteUnit);
      }
      res += unit.getSize();
    } else {
      throw new RuntimeException("bug");
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

  public long remove(TmpCacheUnit unit, boolean isActual) {
    fakeRemove(unit);
    return remove0(unit, isActual);
  }

  public long remove(TmpCacheUnit unit) {
    return remove(unit, true);
  }

  public abstract void fakeAccess(TmpCacheUnit unit);

  public abstract void fakeRemove(TmpCacheUnit unit);

  public abstract TmpCacheUnit getEvictUnit();

  public abstract TmpCacheUnit getMaxPriorityUnit();

  public abstract void evict();

  public abstract void removeByShare(TmpCacheUnit deleteUnit);

  public long cheatAccess(TmpCacheUnit unit) {
    fakeAccess(unit);
    return cheatAccess0(unit);
  }
}
