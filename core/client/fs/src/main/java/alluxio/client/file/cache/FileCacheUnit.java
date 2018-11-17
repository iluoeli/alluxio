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

import alluxio.AlluxioURI;
import alluxio.Client;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.cache.struct.DoubleLinkedList;
import alluxio.client.file.cache.struct.LongPair;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheHitCalculator;
import alluxio.exception.AlluxioException;
import com.google.common.base.Preconditions;
import com.sun.prism.shader.Solid_TextureYV12_AlphaTest_Loader;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static alluxio.client.file.cache.ClientCacheContext.mREadUnlockList;
import static alluxio.client.file.cache.ClientCacheContext.mReadLockList;

public class FileCacheUnit {
  private DoubleLinkedList<CacheInternalUnit> cacheList;
  private long mFileId;
  public LinkedFileBucket mBuckets;
  private boolean use_bucket;
  private final ClientCacheContext.LockManager mLockManager;
  private final ClientCacheContext mContext;

  public FileCacheUnit(long fileId, long length, ClientCacheContext mContext) {
    mFileId = fileId;
    this.mContext = mContext;
    use_bucket = mContext.USE_INDEX_0;
    cacheList = new DoubleLinkedList<>(new CacheInternalUnit(0, 0, -1));
    if (use_bucket) {
      mBuckets = new LinkedFileBucket(length, fileId, mContext);
    }
    this.mLockManager = mContext.getLockManager();

  }

  public DoubleLinkedList<CacheInternalUnit> getCacheList() {
    return cacheList;
  }

  public CacheUnit getKeyFromBucket(long begin, long end, LockTask task) {
    return mBuckets.find(begin, end, task);
  }

  public void cacheCoinFiliter(PriorityQueue<LongPair> queue, Queue<LongPair> tmpQueue) {
    long maxEnd = -1;
    long minBegin = -1;
    while (!queue.isEmpty()) {
      LongPair tmpUnit = queue.poll();
      if (minBegin == -1) {
        minBegin = tmpUnit.getKey();
        maxEnd = tmpUnit.getValue();
      } else {
        if (tmpUnit.getKey() <= maxEnd) {
          maxEnd = Math.max(tmpUnit.getValue(), maxEnd);
        } else {
          tmpQueue.add(new LongPair(minBegin, maxEnd));
          minBegin = tmpUnit.getKey();
          maxEnd = tmpUnit.getValue();
        }
      }
    }
    tmpQueue.add(new LongPair(minBegin, maxEnd));
  }

  private void deleteOldData(CacheInternalUnit unit) {

  }

  private void cacheNewDate(TempCacheUnit unit) {

  }
  /*
  public void cacheCoinFiliter1(Queue<CacheUnit> queue,
                               Queue<CacheUnit> tmpQueue ){
    TempCacheUnit tmpBefore = null;
    long begin = 0;
    long end = 0;
    while(!queue.isEmpty()) {
      CacheUnit tmpUnit = queue.poll();
      boolean isOld = tmpUnit instanceof CacheInternalUnit;
      if(tmpBefore != null) {
        if(tmpUnit.getBegin() >= tmpBefore.getEnd()) {
          if(tmpBefore.isInternal()) {
            deleteOldData(tmpBefore.mCacheConsumer.poll());
            tmpBefore = null;
          } else {
            cacheNewDate(tmpBefore);
          }
          tmpBefore = new TempCacheUnit(tmpUnit.getBegin(),
            tmpUnit.getEnd(), tmpUnit.getFileId());
          if(isOld) {
            tmpBefore.addResource((CacheInternalUnit)tmpUnit);
          } else {
            tmpBefore.addCacheIndex((BaseCacheUnit) tmpUnit);
          }
        } else {
          if(isOld) {
            tmpBefore.addResource((CacheInternalUnit)tmpUnit);
          } else {
            tmpBefore.addCacheIndex((BaseCacheUnit) tmpUnit);
          }
        }
      } else {
        tmpBefore = new TempCacheUnit(tmpUnit.getBegin(),
          tmpUnit.getEnd(), tmpUnit.getFileId());
        if(isOld) {
          tmpBefore.addResource((CacheInternalUnit)tmpUnit);
        }
      }
    }

    if(tmpBefore.isInternal()) {
      deleteOldData(tmpBefore.mCacheConsumer.poll());
    } else {
      cacheNewDate(tmpBefore);
    }

  }*/

  public void cacheCoinFiliter(Queue<CacheUnit> queue, Queue<LongPair> tmpQueue) {
    long maxEnd = -1;
    long minBegin = -1;
    while (!queue.isEmpty()) {
      CacheUnit tmpUnit = queue.poll();
      if (minBegin == -1) {
        minBegin = tmpUnit.getBegin();
        maxEnd = tmpUnit.getEnd();
      } else {
        if (tmpUnit.getBegin() <= maxEnd) {
          maxEnd = Math.max(tmpUnit.getEnd(), maxEnd);
        } else {
          tmpQueue.add(new LongPair(minBegin, maxEnd));
          minBegin = tmpUnit.getBegin();
          maxEnd = tmpUnit.getEnd();
        }
      }
    }
    tmpQueue.add(new LongPair(minBegin, maxEnd));
  }

  public boolean testFinish(TempCacheUnit unit) {
    int i = 0;
    int index = 0;
    while (i < unit.getSize()) {
      i += unit.mData.get(index).capacity();
      index++;
      if (index >= unit.mData.size()) {
        break;
      }
    }
    if (i < unit.getSize()) {
      throw new RuntimeException("unFinish data unit!");
    } else {
    }
    return true;
  }

  public void testCapacity() {
    int space = 0;
    Iterator<CacheInternalUnit> i = cacheList.iterator();
    while (i.hasNext()) {
      space += i.next().getSize();
    }
    System.out.println(space / (1024 * 1024));
  }

  private CacheInternalUnit deleteCache(CacheInternalUnit current) {
    CacheInternalUnit next = current.after;
    current.clearData();
    mBuckets.delete(current);
    cacheList.delete(current);
    //newSize -= current.getSize();
    current = null;
    return next;
  }

  /**
   * Cache part of file data, using for test cases basically.
   */
  public void cache(AlluxioURI uri, long begin, long end) throws Exception {
    FileSystem fs = FileSystem.Factory.get(true);
    FileInStream in = fs.openFile(uri);
    LockTask task = new LockTask(mLockManager, mFileId);
    CacheUnit newUnit = getKeyFromBucket(begin, end, task);
    if (newUnit instanceof TempCacheUnit) {
      TempCacheUnit tmpUnit = (TempCacheUnit) newUnit;
      tmpUnit.setInStream((FileInStreamWithCache) in);
      mContext.getCacheManager().cache(tmpUnit, begin, (int) (end - begin), this);
    }
    task.unlockAll();
  }

  private void check(CacheInternalUnit unit) {
    int tmp = 0;
    for (ByteBuf b : unit.mData) {
      tmp += b.capacity();
    }
    Preconditions.checkArgument(tmp == unit.getSize());
  }

  public long merge0(AlluxioURI uri, Queue<LongPair> tmpQueue) throws IOException, AlluxioException {
    LockTask task = new LockTask(mLockManager, mFileId);
    FileSystem fs = FileSystem.Factory.get(true);
    FileInStream in = fs.openFile(uri);
    long newSize = 0;
    CacheInternalUnit curr = cacheList.head.after;

    while (!tmpQueue.isEmpty()) {
      LongPair l = tmpQueue.poll();
      while (curr != null && curr.getEnd() < l.getKey()) {
        task.deleteLock(curr);
        curr = deleteCache(curr);
      }
      task.deleteUnlock();

      if (l.getKey() - l.getValue() == 0) continue;
      CacheUnit newUnit = getKeyFromBucket(l.getKey(), l.getValue(), task);
      if (newUnit instanceof TempCacheUnit) {
        TempCacheUnit tmpUnit = (TempCacheUnit) newUnit;
        tmpUnit.setInStream((FileInStreamWithCache) in);
        int len = (int) (l.getValue() - l.getKey());
        int res = mContext.getCacheManager().cache(tmpUnit, l.getKey(), len, this);
        if (res != len) {
          // the end of file
          //tmpUnit.resetEnd((int) mLength);
        }
        newSize += res;

        curr = addCache(tmpUnit).after;
        task.unlockAll();
      } else {
        // TODO split the CacheInternalUnit
        curr = ((CacheInternalUnit) newUnit).after;
        task.unlockAll();
      }
    }
    while (curr != null) {
      task.deleteLock(curr);
      curr = deleteCache(curr);
    }
    task.deleteUnlock();
    return newSize;

  }

  /**
   * Return the new Cache size promoted from under_fs
   */
  public long merge(AlluxioURI uri, Queue<CacheUnit> queue) throws IOException, AlluxioException {
    Queue<LongPair> tmpQueue = new LinkedBlockingQueue<>();
    cacheCoinFiliter(queue, tmpQueue);
    return merge0(uri, tmpQueue);
  }


  public long elimiate(Set<CacheUnit> input) {
    long deleteSizeSum = 0;
    HashMap<CacheUnit, PriorityQueue<LongPair>> tmpMap = new HashMap<>();
    LockTask task = new LockTask(mLockManager, mFileId);
    for (CacheUnit unit : input) {
      CacheInternalUnit cache = null;
      try {
        cache = (CacheInternalUnit) getKeyFromBucket(unit.getBegin(), unit.getEnd(), task);
        task.unlockAllReadLocks();
        if (!tmpMap.containsKey(cache)) {
          PriorityQueue<LongPair> queue = new PriorityQueue<>(new Comparator<LongPair>() {
            @Override
            public int compare(LongPair o1, LongPair o2) {
              return (int) (o1.getKey() - o2.getKey());
            }
          });
          LongPair p = new LongPair(unit.getBegin(), unit.getEnd());
          queue.add(p);
          tmpMap.put(cache, queue);
        } else {
          tmpMap.get(cache).add(new LongPair(unit.getBegin(), unit.getEnd()));
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    CacheInternalUnit current = cacheList.head.after;
    Queue<LongPair> tmpQueue = new LinkedBlockingQueue<>();
    while (current != null) {
      CacheInternalUnit next = null;
      boolean deleteAll = false;
      long deleteSize;
      task.deleteLock(current);
      try {
        if (tmpMap.containsKey(current)) {
          cacheCoinFiliter(tmpMap.get(current), tmpQueue);
          current.split(tmpQueue, mBuckets);
          tmpQueue.clear();
        } else {
          deleteAll = true;
        }
        if (!deleteAll) {
          deleteSize = current.getDeleteSize();
        } else {
          deleteSize = current.getSize();
          if (mContext.mUseGhostCache) {
            mContext.getGhostCache().add(new BaseCacheUnit(current.getBegin(), current.getEnd(), mFileId));
          }
        }
        next = current.after;
        if (deleteSize > 0) {
          mBuckets.delete(current);
          deleteSizeSum += deleteSize;
          cacheList.delete(current);
          current.clearData();
          current = null;
        }
      } finally {
        task.deleteUnlock();
        current = next;
      }
    }
    tmpMap.clear();
    tmpMap = null;
    return deleteSizeSum;
  }

  public CacheInternalUnit addCache(TempCacheUnit unit) {
    CacheInternalUnit result = unit.convert();
    while (!unit.deleteQueue.isEmpty()) {
      CacheInternalUnit unit1 = unit.deleteQueue.poll();
      mBuckets.delete(unit1);
      unit1.clearData();
      unit1.before = unit1.after = null;
      unit1 = null;
    }

    cacheList.insertBetween(result, result.before, result.after);
    if (use_bucket) {
      mBuckets.add(result);
    }
    return result;
  }

  public void print() {
    System.out.println("cache list info : ");
    Iterator<CacheInternalUnit> i = cacheList.iterator();
    while (i.hasNext()) {
      CacheInternalUnit u = i.next();
      System.out.println(u.toString());
      u.printtest();
    }
    System.out.println();
    mBuckets.print();
  }
}
