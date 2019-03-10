package alluxio.client.file.cache.submodularLib.cacheSet;

import alluxio.client.file.cache.*;
import alluxio.client.file.cache.submodularLib.SubmodularSetUtils;

import java.util.*;

public class HitCalculator extends CacheHitCalculator {
  private ClientCacheContext mContext = new ClientCacheContext(false);
  private ClientCacheContext mBaseContext = new ClientCacheContext(false);

  public HitCalculator(SubmodularSetUtils utils) {
    super(utils);
  }

  @Override
  public void addMaxBase(CacheUnit maxUnit) {
    CacheUnit unit = mBaseContext.getCache(maxUnit.getFileId(), mFileIdToLength.get(maxUnit.getFileId()), maxUnit.getBegin(),
      maxUnit.getEnd(), mLockTask);
    CacheInternalUnit res;

    if (!unit.isFinish()) {
      TempCacheUnit unit1 = (TempCacheUnit)unit;
      res = mBaseContext.addCache(unit1);
    } else {
      res = (CacheInternalUnit) unit;
    }
    res.accessRecord.add((BaseCacheUnit)maxUnit);
    mSpaceSize += mCurrentMaxSize;
  }


  public void countHitRatio(BaseCacheUnit unit) {
    CacheUnit unit1 = mContext.getCache(unit.getFileId(), mFileIdToLength.get(unit.getFileId()), unit.getBegin(), unit.getEnd(), mLockTask);
    CacheInternalUnit res;
    if (!unit1.isFinish()) {
      res = mContext.addCache((TempCacheUnit) unit1);
    } else {
      res = (CacheInternalUnit) unit1;
    }
    boolean isIn = changeHitValue(res.accessRecord, unit, null);
    if (!isIn) {
      res.accessRecord.add(unit);
    }
  }

  /**
   * @return true if the baseUnit in accessrecord of CacheInternalUnit
   */
  public static boolean changeHitValue(Collection<BaseCacheUnit> set, BaseCacheUnit current, Set<BaseCacheUnit> changeSet) {
    Iterator<BaseCacheUnit> iter = set.iterator();
    boolean isIn = false;
    while (iter.hasNext()) {
      BaseCacheUnit tmp = iter.next();
      if (current.getEnd() <= tmp.getBegin()) {
        break;
      }
      if (current.isCoincience(tmp)) {
        changeHitValue0(current, tmp, changeSet);
      } else if (current.getBegin() == tmp.getBegin() && current.getEnd() == tmp.getEnd()) {
        //tmp.setCurrentHitVal(tmp.getHitValue() + 1);
        //current.setCurrentHitVal(1);
       // changeSet.add(tmp);
        isIn = true;
      }
    }
    return isIn;
  }

  private static void changeHitValue0(BaseCacheUnit unit, BaseCacheUnit tmp, Set<BaseCacheUnit> changeSet) {
    long coinEnd = Math.min(tmp.getEnd(), unit.getEnd());
    long coinBegin = Math.max(tmp.getBegin(), unit.getBegin());
    int coincideSize = (int) (coinEnd - coinBegin);
    double coinPerNew = coincideSize / (double) (unit.getEnd() - unit.getBegin());
    double coinPerOld = coincideSize / (double) (tmp.getEnd() - tmp.getBegin());
    tmp.setCurrentHitVal(tmp.getHitValue() + Math.min(coinPerOld, 1) * 1);
    if (changeSet != null) {
      changeSet.add(tmp);
    }
    if (coinPerNew < 1) {
      unit.setCurrentHitVal(unit.getHitValue() + coinPerNew * 1);
      if (changeSet != null) {
        changeSet.add(unit);
      }
    }
  }

  private double getOverlapRatio(CacheUnit unit, CacheUnit tmp) {
    long coinEnd = Math.min(tmp.getEnd(), unit.getEnd());
    long coinBegin = Math.max(tmp.getBegin(), unit.getBegin());
    int coincideSize = (int) (coinEnd - coinBegin);
    return  coincideSize / (double) (unit.getEnd() - unit.getBegin());
  }

  @Override
  double statisticsHitRatio(CacheUnit unit) {
    CacheUnit unit1 = mBaseContext.getCache(unit.getFileId(), mFileIdToLength.get(unit.getFileId()),unit.getBegin(), unit.getEnd(), mLockTask);
    if (unit1.isFinish()) {
      return 0;
    } else {
      double res = unit.getHitValue();
      TempCacheUnit unit2 = (TempCacheUnit)unit1;
      long size = 0;
      for (CacheInternalUnit unit3 : unit2.mCacheConsumer) {
        size += unit3.getSize();
        for (BaseCacheUnit unit4 : unit3.accessRecord) {
          if (unit4.isCoincience(unit)) {
            res -= getOverlapRatio(unit, unit4);
          }
        }
      }
      mCurrentIncreaseSize = unit2.getSize() - size;
      mSpaceSize += mCurrentIncreaseSize;
      return res;
    }
  }


  @Override
  public void backspace() {
    mSpaceSize -= mCurrentIncreaseSize;
    mCurrentIncreaseSize = 0;
  }

  @Override
  public void init() {
    mCurrentIncreaseSize = mCurrentMaxSize = mSpaceSize = 0;
  }

  @Override
  public void setMaxMark() {
    mCurrentMaxSize = mCurrentIncreaseSize;
  }

  @Override
  public void initSpaceCalculate() {}

  @Override
  public void iterateInit() {}

  @Override
  public void clear() {
    //mBaseContext.clear();
    mBaseContext.mFileIdToInternalList.clear();
  }
}
