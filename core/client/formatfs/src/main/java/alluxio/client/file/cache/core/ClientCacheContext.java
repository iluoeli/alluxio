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

package alluxio.client.file.cache.core;

import alluxio.AlluxioURI;
import alluxio.client.file.CacheParamSetter;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.Metric.HitRatioMetric;
import alluxio.client.file.cache.buffer.MemoryAllocator;
import alluxio.client.file.cache.remote.grpc.service.Data;
import alluxio.client.file.cache.struct.DoubleLinkedList;
import alluxio.client.file.cache.struct.LongPair;
import alluxio.client.file.cache.struct.RBTree;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import alluxio.exception.AlluxioException;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ClientCacheContext implements CacheContext {
  public static final ClientCacheContext INSTANCE = new ClientCacheContext(true);
  public static final ClientCacheContext UTILS = new ClientCacheContext(false);
  public static long mPromotionThreadId = 0;
  public static long fileId;
//  public static final int CACHE_SIZE = 1048576;
  public static int CACHE_SIZE = CacheParamSetter.CACHE_SIZE;
  public final int BUCKET_LENGTH = 10;
//  public static final String mCacheSpaceLimit = "1g";
  public static final String mCacheSpaceLimit = CacheParamSetter.mCacheSpaceLimit;
//  private final long mCacheLimit = getSpaceLimit();
  public static  long mCacheLimit = getSpaceLimit();
  public boolean REVERSE = true;
  public boolean USE_INDEX_0 = true;
  private static final CacheManager mCacheManager;
  private volatile boolean mAllowCache = true;
  private final LockManager mLockManager;
  public boolean mUseGhostCache = false;
  public static final MemoryAllocator mAllocator;
  public static LockTask readTask;

  public static boolean useNettyMemoryUtils = false;

  public static GhostCache getGhostCache() {
    return GhostCache.INSTANCE;
  }

  public ExecutorService COMPUTE_POOL = Executors.newFixedThreadPool(10);
  private static final MetedataCache metedataCache;
  public static long checkout = 0;
  public static long missSize;
  public static long hitTime;
//  public final MODE mode = MODE.EVICT;
  public final MODE mode = CacheParamSetter.mode;
  public static boolean useMetedata = true;
  public long mUsedCacheSpace = 0;
  public double mHitvalue;
  public static int allHitTime = 0;
  public static int missTime = 0;
  public static ConcurrentHashMap<Long, Long> timeMap = new ConcurrentHashMap<>();


  public ExecutorService getThreadPool() {
    return COMPUTE_POOL;
  }

  public enum MODE {
    PROMOTE, EVICT
  }

  public static void release(ByteBuf buf) {
    if (useNettyMemoryUtils) {
      ReferenceCountUtil.release(buf);
    } else {
      mAllocator.release(buf);
    }
  }

  public ClientCacheContext(boolean isReal) {
    if (isReal) {
      mLockManager = new RWLockManager();
    } else {
      mLockManager = new FakeLockManager();
    }
  }

  public long getCacheLimit() {
    return mCacheLimit;
  }

  public MetedataCache getMetedataCache() {
    return metedataCache;
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
    mAllocator = new MemoryAllocator();
    if (!useNettyMemoryUtils)
      ClientCacheContext.mAllocator.init();
  }

  public LockTask getLockTask(long fileId) {
    return new LockTask(mLockManager, fileId);
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
        unit = new FileCacheUnit(fileId, metedataCache.getStatus(fileId).getLength(), this);
        mFileIdToInternalList.put(fileId, unit);
      }
      res += unit.merge(metedataCache.getUri(fileId), cacheSet.sortCacheMap.get(fileId));
    }
    return res;
  }

  public void mergeTest(CacheSet cacheSet, ClientCacheContext context) throws IOException, AlluxioException {
    for (long fileId : cacheSet.keySet()) {
      FileCacheUnit unit = context.mFileIdToInternalList.get(fileId);
      if (unit == null) {
        System.out.println("test1  " + fileId );
        unit = new FileCacheUnit(fileId, 1024 * 1024 * 1024, context);
        context.mFileIdToInternalList.put(fileId, unit);
      }
      unit.mergeTest(cacheSet.sortCacheMap.get(fileId));
    }
  }

  public long getAllSize(CacheSet cacheSet, ClientCacheContext context) throws IOException, AlluxioException {
    long size = 0;
    for (long fileId : cacheSet.keySet()) {
      FileCacheUnit unit = context.mFileIdToInternalList.get(fileId);
      if (unit == null) {
        unit = new FileCacheUnit(fileId, 1024 * 1024 * 1200, context);
        context.mFileIdToInternalList.put(fileId, unit);
      }
      Queue<LongPair> tmpQueue = new LinkedBlockingQueue<>();
      unit.cacheCoinFiliter(cacheSet.sortCacheMap.get(fileId), tmpQueue);
      while (!tmpQueue.isEmpty()) {
        LongPair p = tmpQueue.poll();
        size += (p.getValue() - p.getKey());
      }
    }
    return size;
  }

  public long getAllSize(ClientCacheContext context) {
    long res = 0;
    for(long fileId : context.mFileIdToInternalList.keySet()) {
      FileCacheUnit unit = context.mFileIdToInternalList.get(fileId);
      Iterator<CacheInternalUnit> iterator = unit.getCacheList().iterator();
      while (iterator.hasNext()) {
        res += iterator.next().getSize();
      }
    }
    return res;
  }

  @Override
  public Data convertData(ByteBuf data) {
    return Data.newBuilder().setData(ByteString.copyFrom(data.array())).build();
  }

  @Override
  public CacheUnit getCache(long fileId, long begin, long end) {
    long fileLength = metedataCache.getFileLength(fileId);
    return getCache(fileId, fileLength, begin, end, new UnlockTask());
  }

  public void cache(AlluxioURI uri, long begin, long end) throws Exception {
    FileCacheUnit unit = mFileIdToInternalList.get(fileId);
    if (unit == null) {
      unit = new FileCacheUnit(fileId, metedataCache.getStatus(fileId).getLength(), this);
      mFileIdToInternalList.put(fileId, unit);
    }
    unit.cache(uri, begin, end);
  }

  public CacheUnit getCache(long fileId, long length, long begin, long end, LockTask task) {
    FileCacheUnit unit = mFileIdToInternalList.get(fileId);
    if (unit == null) {
      unit = new FileCacheUnit(fileId, length, this);
      mFileIdToInternalList.put(fileId, unit);
    }
    if (USE_INDEX_0) {
      CacheUnit u = unit.getKeyFromBucket(begin, end, task);
      return u;
    }
    if (!REVERSE) {
      CacheUnit u = getKey2(begin, end, fileId, task);
      return u;
    } else {
      CacheUnit u =  getKeyByReverse2(begin, end, fileId, -1, task);
      return u;
    }
  }

  @SuppressWarnings("unchecked")
  public CacheUnit getCache(URIStatus status, long begin, long end) {
    return getCache(status.getFileId(), status.getLength(), begin, end, null);
  }

  /**
   * Return true if the unit is equal to one element in RBTree.
   */
  public CacheUnit getKeyByTree(long begin, long end, RBTree<CacheInternalUnit> tree, long fileId,
                                int index, LockTask task) {
    CacheInternalUnit x = (CacheInternalUnit) tree.mRoot;
    TempCacheUnit unit = new TempCacheUnit(begin, end, fileId);
    while (x != null) {
      if (begin >= x.getBegin() && end <= x.getEnd()) {
        return x;
      } else if (begin >= x.getEnd()) {
        if (x.right != null) {
          x = x.right;
        } else {
          if (x.after == null || x.after.getBegin() >= end) {
            return handleUnCoincidence(unit, x, x.after, index, task);
          } else {
            if (x.after.getBegin() <= begin) {
              task.readLock(x.after.mBucketIndex);
              return x = x.after;
            }
            return handleLeftCoincidence(x.after, unit, true, index, task);
          }
        }
      } else if (end <= x.getBegin()) {
        if (x.left != null) {
          x = x.left;
        } else {
          if (x.before == null || x.before.getEnd() <= begin) {
            return handleUnCoincidence(unit, x.before, x, index, task);
          } else {
            if (x.before.getEnd() >= end) {
              //((LinkedFileBucket.RBTreeBucket) mFileIdToInternalList.get(fileId).mBuckets.mCacheIndex0[index]).mCacheIndex1.print();
              task.readLock(x.before.mBucketIndex);
              return x = x.before;
            }
            return handleRightCoincidence(unit, x.before, true, index, task);
          }
        }
      } else {
        boolean change = false;
        if (unit.getEnd() > x.getEnd()) {
          unit = (TempCacheUnit)handleLeftCoincidence(x, unit, true, index, task);
          change = true;
        }
        if (unit.getBegin() < x.getBegin()) {
          if (change) unit.mCacheConsumer.removeFirst();
          unit = (TempCacheUnit)handleRightCoincidence(unit, x, true, index, task);
        }
        return unit;
      }
    }
    return unit;
  }

  public CacheUnit getKeyByReverse(long begin, long end, long fileId, PreviousIterator iter,
                                   int bucketIndex, LockTask task) {
    TempCacheUnit newUnit = new TempCacheUnit(begin, end, fileId);
    CacheInternalUnit current = null;
    while (iter.hasPrevious()) {
      current = (CacheInternalUnit) iter.previous();
      long left = current.getBegin();
      long right = current.getEnd();
      if (end <= right) {
        if (begin >= left) {
          newUnit = null;
          return current;
        }
        if (end < left) {
          CacheInternalUnit pre = current.before;
          if (pre == null || begin > pre.getEnd()) {
            return handleUnCoincidence(newUnit, current.before, current, bucketIndex, task);
          }
        } else {
          // right coincidence
          // TODO delete this judgement if allow (1,10)(10,20)=>(1,20)
          if (end != left) return handleRightCoincidence(newUnit, current, true, bucketIndex, task);
        }
      } else {
        //TODO change to > if allow (1,10)(10,20)=>(1,20)
        if (begin >= right) {
          return handleUnCoincidence(newUnit, current, current.after, bucketIndex, task);
        } else {
          return handleRightCoincidence(newUnit, current, true, bucketIndex, task);
        }
      }
    }
    return handleUnCoincidence(newUnit, null, current, bucketIndex, task);
  }

  public CacheUnit getKey(long begin, long end, long fileId, Iterator iter, int bucketIndex,
                          LockTask task) {
    TempCacheUnit newUnit = new TempCacheUnit(begin, end, fileId);
    CacheInternalUnit current = null;
    while (iter.hasNext()) {
      current = (CacheInternalUnit) iter.next();
      long left = current.getBegin();
      long right = current.getEnd();
      if (begin >= left) {
        if (end <= right) {
          newUnit = null;
          return current;
        }
        if (begin > right) {
          CacheInternalUnit next = current.after;
          if (next == null || end < next.getBegin()) {
            return handleUnCoincidence(newUnit, current, current.after, bucketIndex, task);
          }
        } /*else if(begin == right){
          //TODO delete this, only for mt
          CacheInternalUnit next = current.after;
          if(next == null)
          return handleUnCoincidence(newUnit, current, current.after);
        }*/ else {
          //left Coincidence
          //TODO delete this judgement if allow (1,10)(10,20)=>(1,20)
          if (begin != right) return handleLeftCoincidence(current, newUnit, true, bucketIndex,
            task);
        }
      } else {
        if (end <= left) {
          return handleUnCoincidence(newUnit, current.before, current, bucketIndex, task);
        } else {
          //left unCoincidence
          return handleLeftCoincidence(current, newUnit, true, bucketIndex, task);
        }
      }
    }
    return handleUnCoincidence(newUnit, current, null, bucketIndex, task);
  }

  public CacheUnit getKey2(long begin, long end, long fileId, LockTask task) {
    DoubleLinkedList<CacheInternalUnit> cacheList = mFileIdToInternalList.get(fileId).getCacheList();
    if (iter == null) iter = cacheList.iterator();
    return getKey(begin, end, fileId, iter, -1, task);
  }

  public CacheUnit getKeyFromBegin(long begin, long end, long fileId, LockTask task) {
    DoubleLinkedList<CacheInternalUnit> cacheList = mFileIdToInternalList.get(fileId).getCacheList();
    return getKey(begin, end, fileId, cacheList.iterator(), 0, task);
  }

  public CacheUnit getKeyByReverse2(long begin, long end, long fileId, int index, LockTask task) {
    DoubleLinkedList<CacheInternalUnit> cacheList = mFileIdToInternalList.get(fileId).getCacheList();
    PreviousIterator iter = cacheList.previousIterator();
    return getKeyByReverse(begin, end, fileId, iter, index, task);
  }

  /**
   * Set the before unit of the TempCacheUnit to the CacheInternal
   */
  private void setBeforeAndLock(CacheInternalUnit before, TempCacheUnit unit, int bucketindex,
                                LockTask task) {
    if(!needWriteLock()) return;
    if (before != null && before.mBucketIndex != bucketindex) {
      mLockManager.writeLock(before.mBucketIndex, bucketindex - 1, task);
    }
  }

  private boolean needWriteLock() {
    if (isPromotion() && Thread.currentThread().getId() != mPromotionThreadId) return false;
    return true;
  }

  private void setAfterAndLock(CacheInternalUnit after, TempCacheUnit unit, int bucketIndex,
                               LockTask task) {
    if(!needWriteLock()) return;
    if (after != null && after.mBucketIndex != bucketIndex) {
      mLockManager.writeLock(bucketIndex + 1, after.mBucketIndex, task);
    }
  }

  private TempCacheUnit handleUnCoincidence(TempCacheUnit unit, CacheInternalUnit before, CacheInternalUnit after,
                                            int bucketIndex, LockTask task) {

    if (bucketIndex != -1 && needWriteLock()) {
      task.lockUpgrade();
      int leftIndex = bucketIndex;
      int rightIndex = bucketIndex;
      if (before != null && before.mBucketIndex != bucketIndex) {
        leftIndex = before.mBucketIndex;
      }
      if (after != null && after.mBucketIndex != bucketIndex) {
        rightIndex = after.mBucketIndex;
      }

      mLockManager.writeLock(leftIndex, rightIndex, task);
    }
    unit.mBefore = before;
    unit.mAfter = after;
    unit.newSize = unit.getSize();
    return unit;
  }

  /**
   * search CacheInternalUnit before current
   */
  public CacheUnit handleRightCoincidence(TempCacheUnit result, CacheInternalUnit current,
                                          boolean addCache, int bucketIndex, LockTask task) {
    if(needWriteLock()) {
      task.lockUpgrade();
    }
    setAfterAndLock(current.after, result, bucketIndex, task);
    long already = 0;
    if (result.getEnd() < current.getEnd()) {
      result.resetEnd(current.getEnd());
    }
    int currentIndex = bucketIndex;
    while (current.before != null && result.getBegin() < current.getBegin()) {
      if (addCache) {
        if (current.mBucketIndex != currentIndex) {
          currentIndex--;
          addReadOrWriteLocks(current, currentIndex, task);
          currentIndex = current.mBucketIndex;
        }
        result.addResourceReverse(current);
        already += current.getSize();
      }
      current = current.before;
    }
    //if (current.before == null) {
    //  setBeforeAndLock(current, result, bucketIndex);
    //}
    if (current.before != null && result.getBegin() <= current.getEnd()) {
      if (addCache) {
        if (current.mBucketIndex != currentIndex) {
          currentIndex--;
          addReadOrWriteLocks(current, currentIndex, task);
        }
        result.addResourceReverse(current);
        already += current.getSize();
      }
      result.resetBegin(current.getBegin());
      setBeforeAndLock(current.before, result, bucketIndex, task);
    } else if (current.before != null) {
      setBeforeAndLock(current, result, bucketIndex, task);
    }
    result.newSize = result.getSize() - already;
    return judgeIfOnlyOne(result);
  }

  private void addReadOrWriteLocks(CacheInternalUnit current, int currentIndex, LockTask task) {
    if (needWriteLock()) {
      mLockManager.writeLock( currentIndex, current.mBucketIndex, task);
    } else {
      mLockManager.readLock( currentIndex, current.mBucketIndex, task);
    }
  }

  private CacheUnit judgeIfOnlyOne(TempCacheUnit unit) {
    //if (unit.mCacheConsumer.size() == 1) {
    //  CacheInternalUnit tmp = unit.mCacheConsumer.peek();
     // if (tmp.getEnd() == unit.getEnd() && tmp.getBegin() == unit.getBegin()) {
     //   unit = null;
    //    return tmp;
    //  }
   // }
    return unit;
  }

  public CacheUnit handleLeftCoincidence(CacheInternalUnit current, TempCacheUnit result,
                                         boolean addCache, int bucketIndex, LockTask task)
  {
    if (needWriteLock()) {
      task.lockUpgrade();
    }
    setBeforeAndLock(current.before, result, bucketIndex, task);
    long already = 0;
    if (result.getBegin() > current.getBegin()) {
      result.resetBegin(current.getBegin());
    }
    int currentIndex = bucketIndex;
    while (current != null && result.getEnd() > current.getEnd()) {
      if (addCache) {
        if (current.mBucketIndex != currentIndex) {
          currentIndex++;
          addReadOrWriteLocks(current, currentIndex, task);
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
          addReadOrWriteLocks(current, currentIndex, task);
        }
        result.addResource(current);
        already += current.getSize();
      }
      result.resetEnd(current.getEnd());
      setAfterAndLock(current.after, result, bucketIndex, task);
    } else if (current != null) {
      setAfterAndLock(current, result, bucketIndex, task);
    }
    result.newSize = result.getSize() - already;
    return judgeIfOnlyOne(result);
  }

  /**
   * add new cache unit to cache list, WITH OUT CACHE CLEAN
   *
   * @param unit
   */
  public CacheInternalUnit addCache(TempCacheUnit unit) {
    CacheInternalUnit iUnit =  mFileIdToInternalList.get(unit.mFileId).addCache(unit);
    return iUnit;
  }

  public void convertCache(TempCacheUnit unit, DoubleLinkedList<CacheInternalUnit> cacheList) {
    CacheInternalUnit result = unit.convertType();
    cacheList.insertBetween(result, unit.mBefore, unit.mAfter);
    //printInfo(unit.mFileId);
  }

  public long computeIncrese(TempCacheUnit unit) {
    long addSize = unit.getSize();
    for (CacheInternalUnit tmp : unit.mCacheConsumer) {
      addSize -= tmp.getSize();
    }

    return addSize;
  }

  public long delete(CacheInternalUnit unit) {
    FileCacheUnit fileCache = mFileIdToInternalList.get(unit.getFileId());
    long deleteSize = unit.getSize();

    fileCache.mBuckets.delete(unit);
    fileCache.getCacheList().delete(unit);
    unit.clearData();
    unit = null;

    return deleteSize;
  }

  public void printInfo(long fileid) {
    DoubleLinkedList<CacheInternalUnit> cacheList = mFileIdToInternalList.get(fileid).getCacheList();
    System.out.println(cacheList.toString());
  }

  public void clear() {
    for (long fileId : mFileIdToInternalList.keySet()) {
      mFileIdToInternalList.get(fileId).clear();
    }
  }

  public LockManager getLockManager() {
    return mLockManager;
  }

  public class FakeLockManager implements LockManager {

    public void lock(){}

    public ReentrantReadWriteLock initBucketLock(long fileId, int bucketIndex){return  null;}

    public void readLock(long fileId, int bucketIndex, String tag){}

    public void readUnlock(long fileId,int bucketIndex){}

    public void readLock( int beginIndex, int EndIndex, LockTask task){}

    public void writeUnlock(long fileId, int bucketIndex){}

    public void writeLock(long fileId, int bucketIndex){}

    public void writeLock(int beginIndex, int EndIndex,  LockTask task){}

    public boolean evictCheck(){return false ;}

    public void evictReadUnlock(){}

    public void deleteLock(CacheInternalUnit unit, LockTask task){}

    public void evictStart(){}

    public void evictEnd(){}

    public boolean testLock(long fileId){return true;}

    public void readUnlockWithRetry(long fileId, int bucketIndex){}
  }

  public interface LockManager {

    public void lock();

    public ReentrantReadWriteLock initBucketLock(long fileId, int bucketIndex);

    public void readLock(long fileId, int bucketIndex, String tag);

    public void readUnlock(long fileId, int bucketIndex);

    public void readLock(int beginIndex, int EndIndex, LockTask task);

    public void writeUnlock(long fileId, int bucketIndex);

    public void writeLock(long fileId, int bucketIndex);

    public void writeLock(int beginIndex, int EndIndex, LockTask task);

    public boolean evictCheck();

    public void evictReadUnlock();

    public void deleteLock(CacheInternalUnit unit, LockTask task);

    public void evictStart();

    public void evictEnd();

    public boolean testLock(long fileId);

    public void readUnlockWithRetry(long fileId, int bucketIndex);
  }

  public class RWLockManager implements LockManager {

    private ReentrantLock tmplock = new ReentrantLock();
    public final ConcurrentHashMap<Long, ConcurrentHashMap<Integer, ReentrantReadWriteLock>>
      mCacheLock = new ConcurrentHashMap<>();
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

    public void writeLock(long fileId, int bucketIndex) {
      ReentrantReadWriteLock l = mCacheLock.get(fileId).get(bucketIndex);
      if (l == null) {
        System.out.print("lock wrong !!!!!! " + bucketIndex);
      }
      l.writeLock().lock();
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
      ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
      m.put(bucketIndex, lock);
      return lock;
    }

    public void readLock(long fileId, int bucketIndex, String tag) {
      ReentrantReadWriteLock lock = mCacheLock.get(fileId).get(bucketIndex);
      lock.readLock().lock();
    }

    public void readUnlock(long fileId, int bucketIndex) {
      ReentrantReadWriteLock l = mCacheLock.get(fileId).get(bucketIndex);
      if (l.getReadLockCount() > 0) {
        l.readLock().unlock();
      }
    }

    public void readUnlockWithRetry(long fileId, int bucketIndex) {
      ReentrantReadWriteLock l = mCacheLock.get(fileId).get(bucketIndex);
      while (l.getReadHoldCount() > 0) {
        l.readLock().unlock();
      }
    }

    public void writeUnlock(long fileId, int bucketIndex) {
      ReentrantReadWriteLock l = mCacheLock.get(fileId).get(bucketIndex);
      while (l.isWriteLockedByCurrentThread()) {
        l.writeLock().unlock();
      }
    }

    public void writeLock(int beginIndex, int EndIndex, LockTask task) {
      synchronized (this) {
        for (int i = beginIndex; i <= EndIndex; i++) {
          task.writeLock(i);
        }
      }
    }

    public synchronized void deleteLock(CacheInternalUnit unit, LockTask task) {
      task.writeLock(unit.mBucketIndex);
      if (unit.before != null && unit.before.mBucketIndex != unit.mBucketIndex) {
        writeLock(unit.before.mBucketIndex, unit.mBucketIndex - 1, task);
      }
      if (unit.after != null && unit.after.mBucketIndex != unit.mBucketIndex) {
        writeLock(unit.mBucketIndex + 1, unit.after.mBucketIndex, task);
      }
    }

    public  void readLock(int beginIndex, int EndIndex, LockTask task) {
      // synchronized(this) {
      for (int i = beginIndex; i <= EndIndex; i++) {
        task.readLock(i);
      }
      // }
    }
  }
}
