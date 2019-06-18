package alluxio.client.file.cache.submodularLib.stream;

import alluxio.client.file.cache.core.*;

import java.util.*;

public class SieveStreamingHandler {
  private LinkedList<Integer> mOSet = new LinkedList<>();
  private final double mEValue;
  private Map<Integer, ClientCacheContext> mBaseMap = new HashMap<>();
  private long mCacheSpace;
  private double mOPTLowbound;
  private double mOPTUpperbound;

  public SieveStreamingHandler(long mLimit) {
    mCacheSpace = mLimit;
    mOPTUpperbound = 0;
    mOPTLowbound = 0;
    mEValue = 0.5;
  }

  public void handle(BaseCacheUnit unit, long fileLength) {
    updateOPTBound(unit);
    updateBaseCache();
    reComputeMaxHit(unit, fileLength);
  }

  private void reComputeMaxHit(BaseCacheUnit unit, long fileLength) {
    for (int i : mOSet) {
      reCompute0(unit, i, fileLength);
    }
  }

  private double reCompute0(BaseCacheUnit unit, int baseIndex, long fileLength) {
    ClientCacheContext baseCache = mBaseMap.get(baseIndex);
    UnlockTask task = new UnlockTask();
    CacheUnit resultUnit = baseCache.getCache(unit.getFileId(), fileLength, unit.getBegin(), unit
      .getEnd(), task);
    int index = baseCache.mFileIdToInternalList.get(unit.getFileId()).mBuckets.getIndex(unit.getBegin(), unit.getEnd());
    LinkedFileBucket.RBTreeBucket bucket = (LinkedFileBucket.RBTreeBucket) baseCache.mFileIdToInternalList.get(unit.getFileId()).mBuckets.mCacheIndex0[index];
    List<CacheInternalUnit> tmpUnits = new ArrayList<>();
    long newSpace = 0;
    boolean isAccessed = true;
    if (resultUnit.isFinish()) {
      CacheInternalUnit resultUnit0 = (CacheInternalUnit) resultUnit;
      tmpUnits.add(resultUnit0);
    } else {
      isAccessed = false;
      TempCacheUnit resultUnit1 = (TempCacheUnit) resultUnit;
      tmpUnits.addAll(resultUnit1.mCacheConsumer);
      long oldSpace = 0;
      for (CacheInternalUnit tmpunit : tmpUnits) {
        oldSpace += tmpunit.getSize();
      }
      newSpace = resultUnit1.getSize() - oldSpace;
    }
    double hitVal = unit.getHitValue();
    boolean finish = false;
    for (CacheInternalUnit resultUnit0 : tmpUnits) {
      if (finish) break;
      for (CacheUnit tmpUnit : resultUnit0.accessRecord) {
        if (unit.getEnd() <= tmpUnit.getBegin()) {
          finish = true;
          break;
        } else if (unit.isCoincience(tmpUnit)) {
          long coinEnd = Math.min(tmpUnit.getEnd(), unit.getEnd());
          long coinBegin = Math.max(tmpUnit.getBegin(), unit.getBegin());
          int coincideSize = (int) (coinEnd - coinBegin);
          double coinPerNew = coincideSize / (double) (unit.getEnd() - unit.getBegin());
          isAccessed = false;
          hitVal -= coinPerNew;

        }
      }
    }
    if (isAccessed) {
      return baseCache.mHitvalue;
    }
    CacheInternalUnit result;
    if ((hitVal / newSpace) >= getLimit(baseIndex, baseCache.mHitvalue, baseCache.mUsedCacheSpace)) {
      if (resultUnit.isFinish()) {
        result = tmpUnits.get(0);
      } else {
        TempCacheUnit resultUnit1 = (TempCacheUnit) resultUnit;
        result = baseCache.addCache(resultUnit1);
      }
      result.accessRecord.add(unit);
      baseCache.mHitvalue += hitVal;
      baseCache.mUsedCacheSpace += newSpace;
    }
    return baseCache.mHitvalue;
  }

  private void updateBaseCache() {
    List<Integer> needRemove = new ArrayList<>();
    int max = 0;
    Iterator<Integer> iter = mOSet.iterator();
    while (iter.hasNext()) {
      int currIndex = iter.next();
      double opt = Math.pow(1 + mEValue, currIndex);
      if (opt < mOPTLowbound || opt > mOPTUpperbound) {
        needRemove.add(currIndex);
      } else {
        max = Math.max(max, currIndex);
      }
    }
    for (int i : needRemove) {
      mOSet.remove(new Integer(i));
      mBaseMap.remove(i);
    }
    double tmp = Math.pow(1 + mEValue, max + 1);
    while (tmp > mOPTLowbound && tmp < mOPTUpperbound) {
      mOSet.push(max);
      mBaseMap.put(max, new ClientCacheContext(false));
      tmp = Math.pow(1 + mEValue, ++max);
    }
  }

  public Map<Long, FileCacheUnit> getOPT() {
    double maxHit = 0;
    int maxIndex = -1;
    for (int i : mBaseMap.keySet()) {
      double currHit = mBaseMap.get(i).mHitvalue;
      if (maxHit < currHit) {
        maxHit = currHit;
        maxIndex = i;
      }
    }
    System.out.println("max hit: " + maxHit);
    return mBaseMap.get(maxIndex).mFileIdToInternalList;
  }

  private void updateOPTBound(BaseCacheUnit unit) {
    mOPTLowbound = Math.max(mOPTLowbound, unit.getHitValue());
    mOPTUpperbound = Math.max(unit.getHitValue() / (double) unit.getSize() * mCacheSpace * 2, mOPTUpperbound);
  }

  private double getLimit(int i, double hitvalue, long usedSpace) {
    double opt = Math.pow(1 + mEValue, i);
    return ((opt / 2) - hitvalue) / (mCacheSpace - usedSpace);
  }
}

