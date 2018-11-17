package alluxio.client.file.cache.submodularLib.cacheSet;

import alluxio.client.file.cache.*;
import alluxio.client.file.cache.struct.DoubleLinkedList;
import alluxio.client.file.cache.struct.LongPair;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DivideGR extends LRUPolicy {
  public PriorityQueue<BaseCacheUnit> hitRatioQueue = new PriorityQueue<>(new Comparator<BaseCacheUnit>() {
    @Override
    public int compare(BaseCacheUnit o1, BaseCacheUnit o2) {
      return (int) ((o1.getHitValue() - o2.getHitValue()) * 10);
    }
  });
  private ClientCacheContext.LockManager mLockManager;
  private PolicyName mPolicyName = PolicyName.DIVIDE_GR;

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

  private void changeHitValue(BaseCacheUnit unit, BaseCacheUnit tmp, Set<BaseCacheUnit> changeSet) {
    long coinEnd = Math.min(tmp.getEnd(), unit.getEnd());
    long coinBegin = Math.max(tmp.getBegin(), unit.getBegin());
    int coincideSize = (int) (coinEnd - coinBegin);
    double coinPerNew = coincideSize / (double) (unit.getEnd() - unit.getBegin());
    double coinPerOld = coincideSize / (double) (tmp.getEnd() - tmp.getBegin());
    tmp.setCurrentHitVal(tmp.getHitValue() + Math.min(coinPerOld, 1) * 1);
    changeSet.add(tmp);
    if (coinPerNew < 1) {
      unit.setCurrentHitVal(unit.getHitValue() + coinPerNew * 1);
      changeSet.add(unit);
    }
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
    FileCacheUnit unit1 = mContext.mFileIdToInternalList.get(current.getFileId());
    Queue<LongPair> q = new LinkedList<>();
    Queue<Set<BaseCacheUnit>> splitQueue = new LinkedList<>();
    cacheCoinFiliter(current.accessRecord, q, splitQueue);
    current.mTmpSplitQueue = splitQueue;
    current.split(q, unit1.mBuckets);

    q.clear();

    long deleteSize = current.getDeleteSize();
    if (deleteSize > 0) {
      unit1.mBuckets.delete(current);
    }
    DoubleLinkedList<CacheInternalUnit> cacheList = unit1.getCacheList();
    cacheList.delete(current);
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
    boolean isIn = false;

    Iterator<BaseCacheUnit> iter = unitQueue.iterator();
    while (iter.hasNext()) {
      BaseCacheUnit tmp = iter.next();
      if (current.getEnd() <= tmp.getBegin()) {
        break;
      }
      if (current.isCoincience(tmp)) {
        changeHitValue(current, tmp, changeSet);
      } else if (current.getBegin() == tmp.getBegin() && current.getEnd() == tmp.getEnd()) {
        tmp.setCurrentHitVal(tmp.getHitValue() + 1);
        current.setCurrentHitVal(1);
        changeSet.add(tmp);
        isIn = true;
      }
    }

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
      hitRatioQueue.add(resUnit);
    }
  }

  @Override
  public long evict() {
    long delete = 0;
    LockTask task = new LockTask(mLockManager);
    while (mNeedDelete > 0) {
      BaseCacheUnit baseUnit = hitRatioQueue.poll();
      task.setFileId(baseUnit.getFileId());
      CacheInternalUnit unit = (CacheInternalUnit) ClientCacheContext.INSTANCE.
        mFileIdToInternalList.get(baseUnit.getFileId()).getKeyFromBucket(baseUnit.getBegin(),
        baseUnit.getEnd(), task);
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
