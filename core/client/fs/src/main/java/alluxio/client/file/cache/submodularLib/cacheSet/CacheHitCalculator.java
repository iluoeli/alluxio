package alluxio.client.file.cache.submodularLib.cacheSet;

import alluxio.client.file.cache.*;
import alluxio.client.file.cache.struct.DoubleLinkedList;
import alluxio.client.file.cache.submodularLib.FunctionCalculator;
import alluxio.client.file.cache.submodularLib.SubmodularSetUtils;
import alluxio.collections.Pair;
import com.google.common.base.Preconditions;
import sun.misc.Cache;

import java.util.*;

public class CacheHitCalculator extends FunctionCalculator<CacheUnit> {
  public HashMap<Long, Map<CacheUnit, Double>> mHitRatioMap;
  private double mOldValue = 0;
  private CacheUnit mOldUnit = null;
  public HashMap<Long, DoubleLinkedList<BaseCacheUnit>> mHitRatioSortMap;
  private BaseCacheUnit mMaxUnit, mCurrtentUnit;
  private HashMap<Long, Pair<BaseCacheUnit, BaseCacheUnit>> mSearchMarkMap;
  public boolean mComputeSpace = false;
  private boolean mSpaceIsComputed = false;
  private double mCurrentIncreaseSize, mCurrentMaxSize;
  public double mSpaceSize;
  private DoubleLinkedList<CacheInternalUnit> spaceUtil;
  private PreviousIterator<CacheInternalUnit> spaceIter;
  public long begin;
  private UnlockTask task = new UnlockTask();

  public CacheHitCalculator(SubmodularSetUtils utils) {
    super(utils);
    mHitRatioMap = new HashMap<>();
    mHitRatioSortMap = new HashMap<>();
    mSearchMarkMap = new HashMap<>();
  }

  public void backspace() {
    if (mOldValue > 0 && mOldUnit != null) {
      mHitRatioMap.get(mOldUnit.getFileId()).put(mOldUnit, mOldValue);
    }
    mOldValue = 0;
    mOldUnit = null;
    if (mComputeSpace) {
      mSpaceIsComputed = false;
      mSpaceSize -= mCurrentIncreaseSize;

    }
  }

  public static void computeHitValue() {
    //TODO compute hit value;
  }

  public void initSpaceCalculate() {
    mCurrentIncreaseSize = 0;
    mSpaceSize = 0;
    spaceUtil = new DoubleLinkedList<>(new CacheInternalUnit(0, 0, -1));
  }

  public static boolean isCoincidence(CacheUnit u1, CacheUnit u2) {
    return ((BaseCacheUnit) u1).isCoincience(u2);
  }

  public HashMap<Long, HashMap<CacheUnit, Double>> copy(HashMap<Long, HashMap<CacheUnit, Double>> m) {
    HashMap<Long, HashMap<CacheUnit, Double>> res = new HashMap<>();
    for (Map.Entry entry : m.entrySet()) {
      long fileId = (long) entry.getKey();
      HashMap<CacheUnit, Double> tmp = (HashMap<CacheUnit, Double>) entry.getValue();
      HashMap<CacheUnit, Double> tmp2 = new HashMap<>();
      tmp2.putAll(tmp);
      res.put(fileId, tmp2);
    }
    return res;
  }

  public void setBaseSet(HashMap<Long, HashMap<CacheUnit, Double>> hitRatioMap) {
    for (Map.Entry entry : hitRatioMap.entrySet()) {
      long fileId = (long) entry.getKey();
      if (!mHitRatioMap.containsKey(fileId)) {
        mHitRatioMap.put(fileId, new TreeMap<>(new Comparator<CacheUnit>() {
          @Override
          public int compare(CacheUnit o1, CacheUnit o2) {
            return (int) (o1.getBegin() - o2.getBegin());
          }
        }));
        mHitRatioSortMap.put(fileId, new DoubleLinkedList<>(new BaseCacheUnit(Long.MAX_VALUE, Long.MAX_VALUE, fileId)));
      } else {
        HashMap<CacheUnit, Double> tmp = (HashMap<CacheUnit, Double>) entry.getValue();
        Map<CacheUnit, Double> treeTmp = mHitRatioMap.get(fileId);
        DoubleLinkedList<BaseCacheUnit> list = mHitRatioSortMap.get(fileId);
        treeTmp.putAll(tmp);
        for (CacheUnit unit : treeTmp.keySet()) {
          list.add((BaseCacheUnit) unit);
        }
      }
    }
    mOldValue = 0;
    mOldUnit = null;
  }

  public void setMaxMark() {
    mMaxUnit = mCurrtentUnit;
    mCurrentMaxSize = mCurrentIncreaseSize;
  }

  public void clear() {
    mSearchMarkMap.clear();
    for (Long l : mSearchMarkMap.keySet()) {
      Pair<BaseCacheUnit, BaseCacheUnit> p = mSearchMarkMap.get(l);
      p = null;
    }
    mHitRatioMap.clear();
    for (Long l : mHitRatioSortMap.keySet()) {
      DoubleLinkedList<BaseCacheUnit> d = mHitRatioSortMap.get(l);
      d.clear();
    }
    mHitRatioSortMap.clear();
    if (spaceUtil != null) spaceUtil.clear();
    spaceUtil = null;
  }

  public void addMaxBase(CacheUnit maxUnit) {
    BaseCacheUnit tmp = mMaxUnit;
    long fileId = maxUnit.getFileId();
    Map<CacheUnit, Double> tmpMap = mHitRatioMap.get(fileId);
    boolean isInsert = false;
    if (tmp.getBegin() == Long.MAX_VALUE) tmp = tmp.after;
    while (tmp != null) {
      if (isCoincidence(maxUnit, tmp)) {
        changeHitValue(maxUnit, tmp, tmpMap);
      }
      if (maxUnit.getBegin() < tmp.getBegin() && !isInsert) {
        DoubleLinkedList<BaseCacheUnit> l = mHitRatioSortMap.get(fileId);
        l.insertBetween((BaseCacheUnit) maxUnit, tmp.before, tmp);
        isInsert = true;
      }
      if (maxUnit.getEnd() <= tmp.getBegin()) {
        break;
      }
      tmp = tmp.after;
    }

    if (!isInsert) {
      DoubleLinkedList<BaseCacheUnit> l = mHitRatioSortMap.get(fileId);
      l.add((BaseCacheUnit) maxUnit);
      mCurrentMaxSize = maxUnit.getSize();
    }

    if (mComputeSpace) {
      PreviousIterator<CacheInternalUnit> tmpSpaceIter = spaceUtil.previousIterator();
      CacheUnit tmpunit = ClientCacheContext.UTILS.getKeyByReverse(maxUnit.getBegin(),
        maxUnit.getEnd(), maxUnit.getFileId(), tmpSpaceIter, -1, task);
      if (!tmpunit.isFinish()) {
        TempCacheUnit newUnit = (TempCacheUnit) tmpunit;
        ClientCacheContext.UTILS.convertCache(newUnit, spaceUtil);
      }
      mSpaceSize += mCurrentMaxSize;
    }

    markInit(fileId);
  }

  public void iterateInit() {
    mSearchMarkMap.clear();
    mOldValue = 0;
    mOldUnit = null;
    if (mSpaceIsComputed) {
      spaceIter = spaceUtil.previousIterator();
    }
  }

  private void markInit(long fileId) {
    DoubleLinkedList<BaseCacheUnit> list = mHitRatioSortMap.get(fileId);
    mCurrtentUnit = list.head;
    mSearchMarkMap.put(fileId, new Pair<>(mCurrtentUnit, list.head));
  }

  public void init() {
    for (Long l : mSearchMarkMap.keySet()) {
      Pair<BaseCacheUnit, BaseCacheUnit> p = mSearchMarkMap.get(l);
      p = null;
    }
    mSearchMarkMap.clear();
    mHitRatioMap.clear();
    for (Long l : mHitRatioSortMap.keySet()) {
      DoubleLinkedList<BaseCacheUnit> d = mHitRatioSortMap.get(l);
      d.clear();
    }
    mHitRatioSortMap.clear();
    if (mComputeSpace) {
      initSpaceCalculate();
    }
  }

  private double changeHitValue(CacheUnit unit, CacheUnit tmp, Map<CacheUnit, Double> tmpMap) {
    double res = 0;
    long coinEnd = Math.min(tmp.getEnd(), unit.getEnd());
    long coinBegin = Math.max(tmp.getBegin(), unit.getBegin());
    int coincideSize = (int) (coinEnd - coinBegin);
    double coinPerNew = coincideSize / (double) (unit.getEnd() - unit.getBegin());
    double coinPerOld = coincideSize / (double) (tmp.getEnd() - tmp.getBegin());

    if (mOldUnit == null || !mOldUnit.equals(tmp)) {
      mOldValue = tmpMap.get(tmp);
      mOldUnit = tmp;
    }
    tmpMap.put(tmp, tmpMap.get(tmp) + Math.min(coinPerOld, 1) * 1);
    res += Math.min(coinPerOld, 1) * 1;
    if (coinPerNew < 1) {
      tmpMap.put(unit, tmpMap.get(unit) + coinPerNew * 1);
      res += coinPerNew;
    }
    return res;
  }

  private double staticHitValue(CacheUnit unit, CacheUnit tmp) {
    long coinEnd = Math.min(tmp.getEnd(), unit.getEnd());
    long coinBegin = Math.max(tmp.getBegin(), unit.getBegin());
    int coincideSize = (int) (coinEnd - coinBegin);
    return coincideSize / (double) (unit.getEnd() - unit.getBegin());
  }

  private void changeSpaceSize(CacheUnit unit, BaseCacheUnit tmp) {
    if (mComputeSpace && !mSpaceIsComputed) {

      CacheUnit tmpunit = ClientCacheContext.UTILS.getKeyByReverse(tmp.getBegin(),
        tmp.getEnd(), tmp.getFileId(), spaceIter, -1, task);
      if (!tmpunit.isFinish()) {
        TempCacheUnit newUnit = (TempCacheUnit) tmpunit;
        mCurrentIncreaseSize = ClientCacheContext.UTILS.computeIncrese(newUnit);
      } else {
        mCurrentIncreaseSize = 0;
      }
      mSpaceIsComputed = true;
      mSpaceSize += mCurrentIncreaseSize;
    }
  }

  private synchronized double statisticsHitRatio(CacheUnit unit) {
    double res = 0;
    long fileId = unit.getFileId();
    Map<CacheUnit, Double> tmpMap;
    BaseCacheUnit mNextUnit = null;
    if (!mHitRatioMap.containsKey(fileId)) {
      tmpMap = new HashMap<>(3000);
      DoubleLinkedList<BaseCacheUnit> list = new DoubleLinkedList<>(new BaseCacheUnit(Long.MAX_VALUE, Long.MAX_VALUE, fileId));
      mHitRatioMap.put(fileId, tmpMap);
      mHitRatioSortMap.put(fileId, list);
      markInit(fileId);
    } else {
      tmpMap = mHitRatioMap.get(fileId);
      if (!mSearchMarkMap.containsKey(fileId)) {
        markInit(fileId);
      }
    }

    mCurrtentUnit = mSearchMarkMap.get(fileId).getFirst();
    mNextUnit = mSearchMarkMap.get(fileId).getSecond();
    // long t = System.currentTimeMillis();

    if (mOldUnit == null || !mOldUnit.equals(unit)) {
      mOldValue = tmpMap.getOrDefault(unit, 0.0);
      mOldUnit = unit;
    }
    // begin += (System.currentTimeMillis() - t);

    boolean isNew = true;
    tmpMap.put(unit, tmpMap.getOrDefault(unit, 0.0) + 1);
    res++;
    BaseCacheUnit tmp;
    boolean newBegin = false;
    if (mNextUnit.getBegin() == Long.MAX_VALUE || unit.getBegin() >= mNextUnit.getBegin()) {
      tmp = mNextUnit;
      newBegin = true;
    } else {
      tmp = mCurrtentUnit;
    }
    BaseCacheUnit pre = null;

    if (tmp.getBegin() == Long.MAX_VALUE) {
      pre = tmp;
      tmp = tmp.after;
    }
    long maxEnd = Math.max(tmp != null ? tmp.getEnd() : 0, unit.getEnd());
    boolean isNewFinish = false;
    while (tmp != null) {
      if (unit.getEnd() <= tmp.getBegin()) {
        if (!newBegin) {
          break;
        } else {
          if (tmp.getBegin() >= maxEnd) {
            mCurrtentUnit = mNextUnit;
            mNextUnit = tmp;
            isNewFinish = true;
            break;
          }
        }
      } else {
        if (unit.getBegin() == tmp.getBegin() && unit.getEnd() == tmp.getEnd()) {
          res += 1;
          if (mOldUnit == null || !mOldUnit.equals(tmp)) {
            mOldValue = tmpMap.get(tmp);
          }
          tmpMap.put(tmp, tmpMap.get(tmp) + 1);
          isNew = false;
        } else if (isCoincidence(unit, tmp)) {
          res += changeHitValue(unit, tmp, tmpMap);
          changeSpaceSize(unit, mCurrtentUnit);
          isNew = false;
        }
      }
      maxEnd = Math.max(maxEnd, tmp.getEnd());
      pre = tmp;
      tmp = tmp.after;
    }
    if (isNew) {
      mCurrentIncreaseSize = unit.getSize();
      mSpaceSize += mCurrentIncreaseSize;
      res += 1;
    }
    if (newBegin) {
      if (!isNewFinish) {
        mCurrtentUnit = mNextUnit;
      }
      mSearchMarkMap.put(fileId, new Pair<>(mCurrtentUnit, pre));
    }
    return res;
  }

  private double statisticsHitRatio(Set<CacheUnit> accessSet, long fileId) {
    double res = 0;
    for (CacheUnit unit : accessSet) {
      res += statisticsHitRatio(unit);
    }
    return res;
  }

  @Override
  public double function(Set<CacheUnit> input) {
    Preconditions.checkArgument(input instanceof CacheSet);
    CacheSet cacheSet = (CacheSet) input;
    long res = 0;
    for (Map.Entry entry : cacheSet.cacheMap.entrySet()) {
      long fileid = (long) entry.getKey();
      res += statisticsHitRatio((Set<CacheUnit>) entry.getValue(), fileid);
    }
    return res;
  }

  @Override
  public double function(CacheUnit e) {
    return statisticsHitRatio(e);
  }

  public void add(BaseCacheUnit unit) {
    long fileId = unit.getFileId();
    DoubleLinkedList<BaseCacheUnit> list;
    if (!mHitRatioSortMap.containsKey(fileId)) {
      list = new DoubleLinkedList<>(new BaseCacheUnit(Long.MAX_VALUE, Long.MAX_VALUE, fileId));
      list.add(unit);
      mHitRatioMap.put(fileId, new HashMap<>());
      mHitRatioSortMap.put(fileId, list);
      markInit(fileId);
    } else {
      try {
        mHitRatioSortMap.get(fileId).add(unit);
      } catch (Exception e) {
        System.out.println(mHitRatioMap.containsKey(fileId) + " " + mHitRatioSortMap.containsKey(fileId));
        e.printStackTrace();
      }

    }
  }

  public double staticHitRatioFoGhost(CacheUnit unit) {
    double res = 0;
    long fileId = unit.getFileId();
    BaseCacheUnit mNextUnit;

    Map<CacheUnit, Double> tmpMap;
    if (!mHitRatioMap.containsKey(fileId)) {
      tmpMap = new HashMap<>(3000);
      DoubleLinkedList<BaseCacheUnit> list = new DoubleLinkedList<>(new BaseCacheUnit(Long.MAX_VALUE, Long.MAX_VALUE, fileId));
      mHitRatioMap.put(fileId, tmpMap);
      mHitRatioSortMap.put(fileId, list);
      markInit(fileId);
    } else {
      if (!mSearchMarkMap.containsKey(fileId)) {
        markInit(fileId);
      }
    }

    mCurrtentUnit = mSearchMarkMap.get(fileId).getFirst();
    mNextUnit = mSearchMarkMap.get(fileId).getSecond();
    BaseCacheUnit tmp;
    boolean newBegin = false;
    if (mNextUnit.getBegin() == Long.MAX_VALUE || unit.getBegin() >= mNextUnit.getBegin()) {
      tmp = mNextUnit;
      newBegin = true;
    } else {
      tmp = mCurrtentUnit;
    }
    BaseCacheUnit pre = null;
    if (tmp.getBegin() == Long.MAX_VALUE) {
      pre = tmp;
      tmp = tmp.after;
    }
    long maxEnd = Math.max(tmp != null ? tmp.getEnd() : 0, unit.getEnd());
    boolean isNewFinish = false;
    while (tmp != null) {
      if (unit.getEnd() <= tmp.getBegin()) {
        if (!newBegin) {
          break;
        } else {
          if (tmp.getBegin() >= maxEnd) {
            mCurrtentUnit = mNextUnit;
            mNextUnit = tmp;
            isNewFinish = true;
            break;
          }
        }
      } else {
        if (unit.getBegin() == tmp.getBegin() && unit.getEnd() == tmp.getEnd()) {
          res += 1;
        } else if (isCoincidence(unit, tmp)) {
          res += staticHitValue(unit, tmp);
        }
      }
      maxEnd = Math.max(maxEnd, tmp.getEnd());
      pre = tmp;
      tmp = tmp.after;
    }

    if (newBegin) {
      if (!isNewFinish) {
        mCurrtentUnit = mNextUnit;
      }
      mSearchMarkMap.put(fileId, new Pair<>(mCurrtentUnit, pre));
    }
    return res;
  }
}
