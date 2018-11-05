/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.cache;

import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.struct.DoubleLinkedList;
import alluxio.client.file.cache.struct.RBTree;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import alluxio.exception.AlluxioException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ClientCacheContext {
  public static final ClientCacheContext INSTANCE = new ClientCacheContext();
  public static long mPromotionThreadId = 0;
  public static long fileId;
  public static long testTime = 0;
  public static long testTime0 = 0;
  public static final int CACHE_SIZE = 1048576;
  public final int BUCKET_LENGTH = 10;
  public static final String mCacheSpaceLimit = "1g";
  private final long mCacheLimit = getSpaceLimit();
  public boolean REVERSE = true;
  public boolean USE_INDEX_0 = true;
  private static final CacheManager mCacheManager;
  private volatile boolean mAllowCache = true;
  private LockManager mLockManager = new RWLockManager();
  public boolean mUseGhostCache = false;

  public static GhostCache getGhostCache() {
    return GhostCache.INSTANCE;
  }

  public ExecutorService COMPUTE_POOL = Executors.newFixedThreadPool(4);
  public static MetedataCache metedataCache;
  public static long checkout = 0;
  public static long missSize;
  public static long hitTime;
  public final MODE mode = MODE.PROMOTE;
  public static boolean useMetedata = true;
  public long mUsedCacheSpace = 0;
  public double mHitvalue;

  enum MODE {
    PROMOTE, EVICT
  }

  public long getCacheLimit() {
    return mCacheLimit;
  }

  public void updateUsedSpace(long changedSize) {
    mUsedCacheSpace += changedSize;
  }

  public boolean isPromotion() {
    return mode == MODE.PROMOTE;
  }

  static {
    metedataCache = new MetedataCache();
    mCacheManager = new CacheManager(INSTANCE);
  }

  public CacheManager getCacheManager() {
    return mCacheManager;
  }

  public synchronized boolean isAllowCache() {
    return mAllowCache;
  }

  public synchronized void stopCache() {
    mAllowCache = false;
  }

  public synchronized void allowCache() {
    mAllowCache = true;
  }

  public static long getSpaceLimit() {
    String num = mCacheSpaceLimit.substring(0, mCacheSpaceLimit.length() - 1);
    char unit = mCacheSpaceLimit.charAt(mCacheSpaceLimit.length() - 1);
    double n = Double.parseDouble(num);
    if (unit == 'M' || unit == 'm') {
      return (long) (n * 1024 * 1024);
    }
    if (unit == 'K' || unit == 'k') {
      return (long) (n * 1024);
    }
    if (unit == 'G' || unit == 'g') {
      return (long) (n * 1024 * 1024 * 1024);
    }
    return (long) n;
  }

  public final ConcurrentHashMap<Long, FileCacheUnit> mFileIdToInternalList = new ConcurrentHashMap<>();

  public void removeFile(long fileId) {
    mFileIdToInternalList.remove(fileId);
  }

  private Iterator iter = null;

  public long merge(CacheSet cacheSet) throws IOException, AlluxioException {
    long res = 0;
    for (long fileId : cacheSet.keySet()) {
      FileCacheUnit unit = mFileIdToInternalList.get(fileId);
      if (unit == null) {
        unit = new FileCacheUnit(fileId, metedataCache.getStatus(fileId).getLength(), mLockManager);
        mFileIdToInternalList.put(fileId, unit);
      }

      res += unit.merge(metedataCache.getUri(fileId), cacheSet.sortCacheMap.get(fileId));
    }
    return res;
  }

  public CacheUnit getCache(long fileId, long length, long begin, long end) {
    FileCacheUnit unit = mFileIdToInternalList.get(fileId);
    if (unit == null) {
      unit = new FileCacheUnit(fileId, length, mLockManager);
      mFileIdToInternalList.put(fileId, unit);
    }
    if (USE_INDEX_0) {
      return unit.getKeyFromBucket(begin, end);
    }
    if (!REVERSE) {
      return getKey2(begin, end, fileId);
    } else {
      return getKeyByReverse2(begin, end, fileId, -1);
    }
  }

  @SuppressWarnings("unchecked")
  public CacheUnit getCache(URIStatus status, long begin, long end) {
    return getCache(status.getFileId(), status.getLength(), begin, end);
  }

  /**
   * Return true if the unit is equal to one element in RBTree.
   */
  public CacheUnit getKeyByTree(long begin, long end, RBTree<CacheInternalUnit> tree, long fileId, int index) {
    CacheInternalUnit x = (CacheInternalUnit) tree.mRoot;
    TempCacheUnit unit = new TempCacheUnit(begin, end, fileId);
    mLockManager.readLock(fileId, index, "getKeyByTree");
    while (x != null) {
      if (begin >= x.getBegin() && end <= x.getEnd()) {
        x.readLock = index;
        return x;
      } else if (begin >= x.getEnd()) {
        if (x.right != null) {
          x = x.right;
        } else {
          if (x.after == null || x.after.getBegin() >= end) {
            return handleUnCoincidence(unit, x, x.after, index);
          } else {
            if (x.after.getBegin() <= begin) {
              x.readLock = x.after.mBucketIndex;
              return x = x.after;
            }
            return handleLeftCoincidence(x.after, unit, true, index);
          }
        }
      } else if (end <= x.getBegin()) {
        if (x.left != null) {
          x = x.left;
        } else {
          if (x.before == null || x.before.getEnd() <= begin) {
            return handleUnCoincidence(unit, x.before, x, index);
          } else {
            if (x.before.getEnd() >= end) {
              ((LinkedFileBucket.RBTreeBucket) mFileIdToInternalList.get(fileId).mBuckets.mCacheIndex0[index]).mCacheIndex1.print();
              x.readLock = x.before.mBucketIndex;
              return x = x.before;
            }
            return handleRightCoincidence(unit, x.before, true, index);
          }
        }
      } else {
        boolean change = false;
        if (unit.getEnd() > x.getEnd()) {
          unit = handleLeftCoincidence(x, unit, true, index);
          change = true;
        }
        if (unit.getBegin() < x.getBegin()) {
          if (change) unit.mCacheConsumer.removeFirst();
          unit = handleRightCoincidence(unit, x, true, index);
        }
        return unit;
      }
    }
    return unit;
  }

  public CacheUnit getKeyByReverse(long begin, long end, long fileId, PreviousIterator iter, int bucketIndex) {
    TempCacheUnit newUnit = new TempCacheUnit(begin, end, fileId);
    mLockManager.readLock(fileId, bucketIndex, "getByReverse");
    CacheInternalUnit current = null;
    while (iter.hasPrevious()) {
      current = (CacheInternalUnit) iter.previous();
      long left = current.getBegin();
      long right = current.getEnd();
      if (end <= right) {
        if (begin >= left) {
          current.readLock = bucketIndex;
          return current;
        }
        if (end < left) {
          CacheInternalUnit pre = current.before;
          if (pre == null || begin > pre.getEnd()) {
            return handleUnCoincidence(newUnit, current.before, current, bucketIndex);
          }
        } else {
          // right coincidence
          // TODO delete this judgement if allow (1,10)(10,20)=>(1,20)
          if (end != left) return handleRightCoincidence(newUnit, current, true, bucketIndex);
        }
      } else {
        //TODO change to > if allow (1,10)(10,20)=>(1,20)
        if (begin >= right) {
          return handleUnCoincidence(newUnit, current, current.after, bucketIndex);
        } else {
          return handleRightCoincidence(newUnit, current, true, bucketIndex);
        }
      }
    }
    return handleUnCoincidence(newUnit, null, current, bucketIndex);
  }

  public CacheUnit getKey(long begin, long end, long fileId, Iterator iter, int bucketIndex) {

    TempCacheUnit newUnit = new TempCacheUnit(begin, end, fileId);
    mLockManager.readLock(fileId, bucketIndex, "getKey");
    CacheInternalUnit current = null;
    while (iter.hasNext()) {
      current = (CacheInternalUnit) iter.next();
      long left = current.getBegin();
      long right = current.getEnd();
      if (begin >= left) {
        if (end <= right) {
          current.readLock = bucketIndex;
          return current;
        }
        if (begin > right) {
          CacheInternalUnit next = current.after;
          if (next == null || end < next.getBegin()) {
            return handleUnCoincidence(newUnit, current, current.after, bucketIndex);
          }
        } /*else if(begin == right){
          //TODO delete this, only for test
          CacheInternalUnit next = current.after;
          if(next == null)
          return handleUnCoincidence(newUnit, current, current.after);
        }*/ else {
          //left Coincidence
          //TODO delete this judgement if allow (1,10)(10,20)=>(1,20)
          if (begin != right) return handleLeftCoincidence(current, newUnit, true, bucketIndex);
        }
      } else {
        if (end <= left) {
          return handleUnCoincidence(newUnit, current.before, current, bucketIndex);
        } else {
          //left unCoincidence
          return handleLeftCoincidence(current, newUnit, true, bucketIndex);
        }
      }
    }
    return handleUnCoincidence(newUnit, current, null, bucketIndex);
  }

  public CacheUnit getKey2(long begin, long end, long fileId) {
    DoubleLinkedList<CacheInternalUnit> cacheList = mFileIdToInternalList.get(fileId).getCacheList();
    if (iter == null) iter = cacheList.iterator();
    return getKey(begin, end, fileId, iter, -1);
  }

  public CacheUnit getKeyFromBegin(long begin, long end, long fileId) {
    DoubleLinkedList<CacheInternalUnit> cacheList = mFileIdToInternalList.get(fileId).getCacheList();
    return getKey(begin, end, fileId, cacheList.iterator(), 0);
  }

  public CacheUnit getKeyByReverse2(long begin, long end, long fileId, int index) {
    DoubleLinkedList<CacheInternalUnit> cacheList = mFileIdToInternalList.get(fileId).getCacheList();
    PreviousIterator iter = cacheList.previousIterator();
    return getKeyByReverse(begin, end, fileId, iter, index);
  }

  private void setBeforeAndLock(CacheInternalUnit before, TempCacheUnit unit, int bucketindex) {
    if (before != null && before.mBucketIndex != bucketindex) {
      mLockManager.writeLock(unit.getFileId(), before.mBucketIndex, bucketindex - 1, unit);
    }
  }

  private void setAfterAndLock(CacheInternalUnit after, TempCacheUnit unit, int bucketIndex) {
    if (after != null && after.mBucketIndex != bucketIndex) {
      mLockManager.writeLock(unit.getFileId(), bucketIndex + 1, after.mBucketIndex, unit);
    }
  }

  private TempCacheUnit handleUnCoincidence(TempCacheUnit unit, CacheInternalUnit before, CacheInternalUnit after, int bucketIndex) {

    if (bucketIndex != -1) {
      mLockManager.readUnlock(unit.getFileId(), bucketIndex);
      int leftIndex = bucketIndex;
      int rightIndex = bucketIndex;
      if (before != null && before.mBucketIndex != bucketIndex) {
        leftIndex = before.mBucketIndex;
      }
      if (after != null && after.mBucketIndex != bucketIndex) {
        rightIndex = after.mBucketIndex;
      }
      mLockManager.writeLock(unit.getFileId(), leftIndex, rightIndex, unit);
    }
    unit.mBefore = before;
    unit.mAfter = after;
    unit.newSize = unit.getSize();
    return unit;
  }

  /**
   * search CacheInternalUnit before current
   */
  public TempCacheUnit handleRightCoincidence(TempCacheUnit result, CacheInternalUnit current, boolean addCache, int bucketIndex) {
    mLockManager.lockUpgrade(result.getFileId(), bucketIndex);
    result.lockedIndex.add(bucketIndex);
    setAfterAndLock(current.after, result, bucketIndex);
    long already = 0;
    if (result.getEnd() < current.getEnd()) {
      result.resetEnd(current.getEnd());
    }
    int currentIndex = bucketIndex;
    while (current.before != null && result.getBegin() < current.getBegin()) {
      if (addCache) {
        if (current.mBucketIndex != currentIndex) {
          currentIndex--;
          mLockManager.writeLock(current.getFileId(), current.mBucketIndex, currentIndex, result);
          currentIndex = current.mBucketIndex;
        }
        result.addResourceReverse(current);
        already += current.getSize();
      }
      current = current.before;
    }
    if (current.before == null) {
      setBeforeAndLock(current, result, bucketIndex);
    }
    if (current.before != null && result.getBegin() <= current.getEnd()) {
      if (addCache) {
        if (current.mBucketIndex != currentIndex) {
          currentIndex--;
          mLockManager.writeLock(current.getFileId(), current.mBucketIndex, currentIndex, result);
        }
        result.addResourceReverse(current);
        already += current.getSize();
      }
      result.resetBegin(current.getBegin());
      setBeforeAndLock(current.before, result, bucketIndex);
    } else if (current.before != null) {
      setBeforeAndLock(current, result, bucketIndex);
    }
    result.newSize = result.getSize() - already;
    return result;
  }

  public TempCacheUnit handleLeftCoincidence(CacheInternalUnit current, TempCacheUnit result, boolean addCache, int bucketIndex) {
    mLockManager.lockUpgrade(result.getFileId(), bucketIndex);
    result.lockedIndex.add(bucketIndex);
    setBeforeAndLock(current.before, result, bucketIndex);
    long already = 0;
    if (result.getBegin() > current.getBegin()) {
      result.resetBegin(current.getBegin());
    }
    int currentIndex = bucketIndex;
    while (current != null && result.getEnd() > current.getEnd()) {
      if (addCache) {
        if (current.mBucketIndex != currentIndex) {
          currentIndex++;
          mLockManager.writeLock(current.getFileId(), currentIndex, current.mBucketIndex, result);
        }
        result.addResource(current);
        already += current.getSize();
      }
      if (current.after == null) {
        result.mAfter = null;
      }
      current = current.after;
    }
    if (current != null && result.getEnd() >= current.getBegin()) {
      if (addCache) {
        if (current.mBucketIndex != currentIndex) {
          currentIndex++;
          mLockManager.writeLock(current.getFileId(), currentIndex, current.mBucketIndex, result);
        }
        result.addResource(current);
        already += current.getSize();
      }
      result.resetEnd(current.getEnd());
      setAfterAndLock(current.after, result, bucketIndex);
    } else if (current != null) {
      setAfterAndLock(current, result, bucketIndex);
    }
    result.newSize = result.getSize() - already;
    return result;
  }

  /**
   * add new cache unit to cache list, WITH OUT CACHE CLEAN
   *
   * @param unit
   */
  public CacheInternalUnit addCache(TempCacheUnit unit) {
    // long beginTime = System.currentTimeMillis();
    return mFileIdToInternalList.get(unit.mFileId).addCache(unit);
    //insertTime += (System.currentTimeMillis() - beginTime);
  }

  public void convertCache(TempCacheUnit unit, DoubleLinkedList<CacheInternalUnit> cacheList) {
    CacheInternalUnit result = unit.convertType();
    cacheList.insertBetween(result, unit.mBefore, unit.mAfter);
    //printInfo(unit.mFileId);
  }

  public long computeIncrese(TempCacheUnit unit) {
    long addSize = 0;
    CacheInternalUnit result = unit.convertType();
    for (CacheInternalUnit tmp = unit.mBefore.after; tmp != null && tmp.after != unit.mAfter; tmp = tmp.after) {
      addSize -= tmp.getSize();
    }
    //printInfo(unit.mFileId);
    addSize += result.getSize();
    return addSize;
  }

  public long delete(CacheInternalUnit unit) {
    FileCacheUnit fileCache = mFileIdToInternalList.get(unit.getFileId());
    long fileId = unit.getFileId();
    int index = unit.mBucketIndex;
    long deleteSize = unit.getSize();
    mLockManager.writeUnlock(fileId, index);
    try {
      fileCache.mBuckets.delete(unit);
      fileCache.getCacheList().remove(unit);
      unit.clearData();
      unit = null;
    } finally {
      mLockManager.writeUnlock(fileId, index);
    }
    return deleteSize;
  }

  public void printInfo(long fileid) {
    DoubleLinkedList<CacheInternalUnit> cacheList = mFileIdToInternalList.get(fileid).getCacheList();
    System.out.println(cacheList.toString());
  }

  public LockManager getLockManager() {
    return mLockManager;
  }

  public class FakeLockManager implements LockManager {
    public void lock() {
    }

    public ReentrantReadWriteLock initBucketLock(long fileId, int bucketIndex) {
      return null;
    }

    public void readLock(long fileId, int bucketIndex, String tag) {
    }

    public void readUnlock(long fileId, int bucketIndex) {
    }

    public ReentrantReadWriteLock getLock(long fileId, int bucketIndex) {
      return null;
    }

    public void writeUnlock(long fileId, int bucketIndex) {
    }

    public void lockUpgrade(long fileId, int index) {
    }

    public void lockUpgrade(long fileId, int beginIndex, int EndIndex, TempCacheUnit unit) {
    }

    public void writeLock(long fileId, int beginIndex, int EndIndex, TempCacheUnit unit) {
    }

    public void writeLock(long fileId, int beginIndex, int EndIndex, ArrayList<ReentrantReadWriteLock> l) {
    }

    public boolean evictCheck() {
      return false;
    }

    public void evictReadUnlock() {
    }

    public void writeUnlockList(long fileId, Collection<Integer> c) {
    }

    public List<ReentrantReadWriteLock> deleteLock(CacheInternalUnit unit) {
      return null;
    }

    public void evictStart() {
    }

    public void evictEnd() {
    }

    public boolean testLock(long fileId) {
      return true;
    }
  }

  public interface LockManager {

    public void lock();

    public ReentrantReadWriteLock initBucketLock(long fileId, int bucketIndex);

    public void readLock(long fileId, int bucketIndex, String tag);

    public void readUnlock(long fileId, int bucketIndex);


    public ReentrantReadWriteLock getLock(long fileId, int bucketIndex);

    public void writeUnlock(long fileId, int bucketIndex);

    public void lockUpgrade(long fileId, int index);

    public void lockUpgrade(long fileId, int beginIndex, int EndIndex, TempCacheUnit unit);

    public void writeLock(long fileId, int beginIndex, int EndIndex, TempCacheUnit unit);

    public void writeLock(long fileId, int beginIndex, int EndIndex, ArrayList<ReentrantReadWriteLock> l);

    public boolean evictCheck();

    public void evictReadUnlock();

    public void writeUnlockList(long fileId, Collection<Integer> c);

    public List<ReentrantReadWriteLock> deleteLock(CacheInternalUnit unit);

    public void evictStart();

    public void evictEnd();

    public boolean testLock(long fileId);
  }

  public static class RWLockManager implements LockManager {

    private ReentrantLock tmplock = new ReentrantLock();
    private final ConcurrentHashMap<Long, ConcurrentHashMap<Integer, ReentrantReadWriteLock>> mCacheLock = new ConcurrentHashMap<>();
    public final ReentrantReadWriteLock evictLock = new ReentrantReadWriteLock();

    public void lock() {
      tmplock.lock();
    }

    public boolean evictCheck() {
      return evictLock.readLock().tryLock();
    }

    public void evictStart() {
      evictLock.writeLock().lock();
    }

    public void evictEnd() {
      evictLock.writeLock().unlock();
    }

    public void evictReadUnlock() {
      evictLock.readLock().unlock();
    }

    public void unlock() {
      tmplock.unlock();
    }

    public boolean testLock(long fileId) {
      if (!mCacheLock.containsKey(fileId) || !mCacheLock.get(fileId).containsKey(0)) {
        //System.out.println("false");
        return false;
      } else if (mCacheLock.get(fileId).get(0).isWriteLocked()) {
        return true;
      } else {
        return false;
      }
    }

    public ReentrantReadWriteLock initBucketLock(long fileId, int bucketIndex) {
      ConcurrentHashMap<Integer, ReentrantReadWriteLock> m;
      if (mCacheLock.containsKey(fileId)) {
        m = mCacheLock.get(fileId);
        if (m.containsKey(bucketIndex)) {
          return m.get(bucketIndex);
        }
      } else {
        m = new ConcurrentHashMap<>();
        mCacheLock.put(fileId, m);
      }
      ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
      m.put(bucketIndex, lock);
      return lock;
    }

    public synchronized void readLock(long fileId, int bucketIndex, String tag) {
      mCacheLock.get(fileId).get(bucketIndex);
    }

    public void readUnlock(long fileId, int bucketIndex) {
      ReentrantReadWriteLock l = mCacheLock.get(fileId).get(bucketIndex);
      if (l.getReadLockCount() > 0) {
        l.readLock().unlock();
      }
    }

    public ReentrantReadWriteLock getLock(long fileId, int bucketIndex) {
      return mCacheLock.get(fileId).get(bucketIndex);
    }

    public void writeUnlock(long fileId, int bucketIndex) {
      ReentrantReadWriteLock l = mCacheLock.get(fileId).get(bucketIndex);
      l.writeLock().unlock();
    }

    public void writeUnlockList(long fileId, Collection<Integer> c) {
      for (int i : c) {
        writeUnlock(fileId, i);
      }
    }

    public void lockUpgrade(long fileId, int index) {
      ReentrantReadWriteLock lock = mCacheLock.get(fileId).get(index);
      if (lock.getReadLockCount() > 0) {
        //System.out.println("read unlock " + index);
        lock.readLock().unlock();
      }
      lock.writeLock().lock();
    }

    public void lockUpgrade(long fileId, int beginIndex, int EndIndex, TempCacheUnit unit) {
      for (int i = beginIndex; i <= EndIndex; i++) {
        ReentrantReadWriteLock lock = mCacheLock.get(fileId).get(i);
        lock.readLock().unlock();
        lock.writeLock().lock();
        unit.lockedIndex.add(i);
      }
    }

    public void writeLock(long fileId, int beginIndex, int EndIndex, TempCacheUnit unit) {
      synchronized (this) {
        for (int i = beginIndex; i <= EndIndex; i++) {
          ReentrantReadWriteLock lock = mCacheLock.get(fileId).get(i);
          if (lock == null) {
            lock = initBucketLock(fileId, i);
          }
          lock.writeLock().lock();
          unit.lockedIndex.add(i);
        }
      }
    }

    public void writeLock(long fileId, int beginIndex, int EndIndex, ArrayList<ReentrantReadWriteLock> l) {
      synchronized (this) {
        for (int i = beginIndex; i <= EndIndex && i > 0; i++) {
          ReentrantReadWriteLock lock = mCacheLock.get(fileId).get(i);
          lock.writeLock().lock();
          l.add(lock);
        }
      }
    }

    public synchronized List<ReentrantReadWriteLock> deleteLock(CacheInternalUnit unit) {
      ArrayList<ReentrantReadWriteLock> l = new ArrayList<>();
      ReentrantReadWriteLock rwLock = initBucketLock(unit.getFileId(), unit.mBucketIndex);
      rwLock.writeLock().lock();
      l.add(rwLock);
      if (unit.before != null && unit.before.mBucketIndex != unit.mBucketIndex) {
        writeLock(unit.getFileId(), unit.before.mBucketIndex, unit.mBucketIndex - 1, l);
      }
      if (unit.after != null && unit.after.mBucketIndex != unit.mBucketIndex) {
        writeLock(unit.getFileId(), unit.mBucketIndex + 1, unit.after.mBucketIndex, l);
      }
      return l;
    }
  }
}
