package alluxio.client.file.cache.submodularLib.cacheSet;

import alluxio.client.file.cache.core.*;
import alluxio.client.file.cache.struct.DoubleLinkedList;
import alluxio.client.file.cache.struct.LongPair;
import alluxio.client.file.cache.submodularLib.LRUPolicy;

import java.util.*;

import static alluxio.client.file.cache.submodularLib.cacheSet.HitCalculator.changeHitValue;

public class DivideGR extends LRUPolicy {
  public PriorityQueue<BaseCacheUnit> hitRatioQueue = new PriorityQueue<>(new Comparator<BaseCacheUnit>() {
    @Override
    public int compare(BaseCacheUnit o1, BaseCacheUnit o2) {
      return (int) ((o1.getHitValue() - o2.getHitValue()) * 10);
    }
  });
  private ClientCacheContext.LockManager mLockManager;
  private PolicyName mPolicyName = PolicyName.DIVIDE_GR;
  private Set<BaseCacheUnit> deleteSet = new HashSet<>();

  @Override
  public PolicyName getPolicyName() {
    return mPolicyName;
  }

  @Override
  public void init(long cacheSize, ClientCacheContext context) {
    mCacheCapacity = cacheSize;
    mCacheSize = 0;
    mContext = context;
    mLockManager = mContext.getLockManager();
  }

  private void deleteHitValue(BaseCacheUnit unit, BaseCacheUnit tmp, Set<BaseCacheUnit> changeSet) {
    long coinEnd = Math.min(tmp.getEnd(), unit.getEnd());
    long coinBegin = Math.max(tmp.getBegin(), unit.getBegin());
    int coincideSize = (int) (coinEnd - coinBegin);
    double coinPerOld = coincideSize / (double) (tmp.getEnd() - tmp.getBegin());
    tmp.setCurrentHitVal(tmp.getHitValue() + Math.min(coinPerOld, 1) * 1);
    changeSet.add(tmp);
  }

  public long deleteCache(CacheInternalUnit current) {

    FileCacheUnit uu = mContext.mFileIdToInternalList.get(current.getFileId());
    int i = uu.mBuckets.getIndex(current.getBegin(), current.getEnd());
    FileCacheUnit unit1 = mContext.mFileIdToInternalList.get(current.getFileId());
    Queue<LongPair> q = new LinkedList<>();
    Queue<Set<BaseCacheUnit>> splitQueue = new LinkedList<>();
    cacheCoinFiliter(current.accessRecord, q, splitQueue);

    current.mTmpSplitQueue = splitQueue;

    List<CacheInternalUnit> res = current.split(q);

    q.clear();

    long deleteSize = current.getDeleteSize();
    if (deleteSize > 0) {
      unit1.mBuckets.delete(current);
      DoubleLinkedList<CacheInternalUnit> cacheList = unit1.getCacheList();
      cacheList.delete(current);
      for (CacheInternalUnit unit : res) {
        unit1.mBuckets.add(unit);
      }
    }
    /*
    try {
      uu.mBuckets.mCacheIndex0[i].test();
    } catch (RuntimeException e) {
      for (CacheInternalUnit uu1 : res) {
        System.out.println(uu1);
      }
      System.out.println("origin : " + current);
      throw e;
    }*/
    return deleteSize;
  }

  public void cacheCoinFiliter(Set<BaseCacheUnit> set, Queue<LongPair> tmpQueue, Queue<Set<BaseCacheUnit>> tmpQueue2) {
    long maxEnd = -1;
    long minBegin = -1;
    Iterator<BaseCacheUnit> iter = set.iterator();
    Set<BaseCacheUnit> s = new HashSet<>();
    while (iter.hasNext()) {
      BaseCacheUnit tmpUnit = iter.next();
      if (minBegin == -1) {
        minBegin = tmpUnit.getBegin();
        maxEnd = tmpUnit.getEnd();
      } else {
        if (tmpUnit.getBegin() <= maxEnd) {
          maxEnd = Math.max(tmpUnit.getEnd(), maxEnd);
        } else {
          tmpQueue.add(new LongPair(minBegin, maxEnd));
          tmpQueue2.add(s);
          s = new HashSet<>();
          minBegin = tmpUnit.getBegin();
          maxEnd = tmpUnit.getEnd();
        }
      }
      s.add(tmpUnit);
    }
    tmpQueue.add(new LongPair(minBegin, maxEnd));
    tmpQueue2.add(s);
  }

  private void addReCompute(CacheInternalUnit unit, BaseCacheUnit current) {

    Set<BaseCacheUnit> unitQueue = unit.accessRecord;
    Set<BaseCacheUnit> changeSet = new HashSet<>();

    boolean isIn = changeHitValue(unitQueue, current, changeSet);

    if (!isIn) {
      unitQueue.add(current);
      changeSet.add(current);
    }

    for (BaseCacheUnit resUnit : changeSet) {
      if (hitRatioQueue.contains(resUnit)) {
        hitRatioQueue.remove(resUnit);
      }
      hitRatioQueue.offer(resUnit);
    }
  }

  private void deleteReCompute(CacheInternalUnit unit, BaseCacheUnit current) {
    Set<BaseCacheUnit> unitQueue = unit.accessRecord;
    Set<BaseCacheUnit> changeSet = new HashSet<>();

    Iterator<BaseCacheUnit> iter = unitQueue.iterator();
    while (iter.hasNext()) {
      BaseCacheUnit tmp = iter.next();
      if (current.getEnd() <= tmp.getBegin()) {
        break;
      }
      if (current.isCoincience(tmp)) {
        deleteHitValue(current, tmp, changeSet);
      } else if (current.getBegin() == tmp.getBegin() && current.getEnd() == tmp.getEnd()) {
        tmp.setCurrentHitVal(tmp.getHitValue() - 1);
        changeSet.add(tmp);
      }
    }

    for (BaseCacheUnit resUnit : changeSet) {
      if (hitRatioQueue.contains(resUnit)) {
        hitRatioQueue.remove(resUnit);
      }
      if (resUnit.getHitValue() != 0) {
        hitRatioQueue.add(resUnit);
      }
    }
  }

  public void test(CacheInternalUnit unit, FileCacheUnit unit1) {
    CacheUnit u = unit1.mBuckets.find(unit.getBegin(), unit.getEnd(), new UnlockTask());
    if (! (u instanceof CacheInternalUnit)) {
      throw new RuntimeException();
    }
  }

  @Override
  public long evict() {
    long delete = 0;
    LockTask task = new LockTask(mLockManager);
    while (mNeedDelete > 0) {
      BaseCacheUnit baseUnit = hitRatioQueue.poll();
      task.setFileId(baseUnit.getFileId());
      CacheUnit unit1 = mContext.mFileIdToInternalList.get(baseUnit.getFileId()).getKeyFromBucket(baseUnit.getBegin(),
        baseUnit.getEnd(), task);
      if (!unit1.isFinish()) {

        System.out.println(baseUnit);
        System.out.println(unit1.toString());
        FileCacheUnit fileCacheUnit = mContext.mFileIdToInternalList.get(baseUnit.getFileId());

        int index =fileCacheUnit.mBuckets.getIndex(baseUnit.getBegin(), baseUnit.getEnd());

        Iterator<CacheInternalUnit> iterator = fileCacheUnit.getCacheList().iterator();
        while(iterator.hasNext()) {
          CacheInternalUnit tmp = iterator.next();
          System.out.print(tmp.getBegin() + " " + tmp.getEnd() + " || ");
        }
        throw new RuntimeException();
      }
      CacheInternalUnit unit = (CacheInternalUnit)unit1;

      task.unlockAllReadLocks();
      // change read lock to write lock
      try {
        task.deleteLock(unit);
        unit.accessRecord.remove(baseUnit);
        deleteReCompute(unit, baseUnit);
        long deletetmp = deleteCache(unit);

        delete += deletetmp;
        mNeedDelete -= deletetmp;
      } finally {
        task.unlockAll();
      }
    }
    return delete;
  }

  @Override
  public synchronized void fliter(CacheInternalUnit unit, BaseCacheUnit current) {
    addReCompute(unit, current);
    unit.accessRecord.add(current);
  }

  @Override
  public void clear() {

  }
}
