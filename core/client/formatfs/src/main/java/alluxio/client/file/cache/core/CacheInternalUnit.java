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

import alluxio.client.file.cache.Metric.ClientCacheStatistics;
import alluxio.client.file.cache.Metric.HitRatioMetric;
import alluxio.client.file.cache.struct.LinkNode;
import alluxio.client.file.cache.struct.LongPair;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.util.*;

public class CacheInternalUnit extends LinkNode<CacheInternalUnit> implements CacheUnit {
  private long mBegin;
  private long mEnd;
  //private ByteBuf mData;
  public List<ByteBuf> mData = new ArrayList<>();
  private long mFileId;
  private final long mSize;
  private long mDeleteCacheSize = 0;
  public double mLastHit = 0;
  public Set<BaseCacheUnit> accessRecord = Collections.synchronizedSet(new TreeSet<>(new Comparator<CacheUnit>() {
    @Override
    public int compare(CacheUnit o1, CacheUnit o2) {
      return (int) (o1.getBegin() - o2.getBegin());
    }
  }));
  public Queue<Set<BaseCacheUnit>> mTmpSplitQueue = null;

  public int mBucketIndex = 0;
  public String mTestName;
  private LockTask mLockTask;
  private boolean isDeleted  = false;


  public long getSize() {
    return mSize;
  }

  public void initBucketIndex(int index) {
    mBucketIndex = index;
  }

  @Override
  public long getFileId() {
    return mFileId;
  }

  public CacheInternalUnit(long begin, long end, long fileId) {
    mBegin = begin;
    mEnd = end;
    mFileId = fileId;
    mSize = mEnd - mBegin;
  }

  public CacheInternalUnit(long begin, long end, long fileId, List<ByteBuf> data) {
    //Preconditions.checkArgument(data.size() > 0);
    mBegin = begin;
    mEnd = end;
    mData = data;
    mFileId = fileId;
    mSize = mEnd - mBegin;
  }

  public LockTask getLockTask() {
    return mLockTask;
  }

  public void setLockTask(LockTask task) {
    mLockTask = task;
  }


  @Override
  public void setCurrentHitVal(double hitValue) {
    mLastHit = hitValue;
  }

  @Override
  public double getHitValue() {
    return mLastHit;
  }

  public long getDeleteSize() {
    return mDeleteCacheSize;
  }

  public long getBegin() {
    return mBegin;
  }

  public long getEnd() {
    return mEnd;
  }

  public boolean isCached() {
    return mData != null;
  }

  public int positionedRead(byte[] b, int off) {
    return positionedRead(b, off, mBegin, (int) (mEnd - mBegin));
  }

  public int test(byte[] b) {
    int pos = 0;
    for(ByteBuf buf : mData) {
      buf.getBytes(0, b, pos, buf.capacity());
      pos += buf.capacity();
    }
    return pos;
  }

  //TODO: need to manage the refcount of returned data.
  @Override
  public List<ByteBuf> get(long pos, long len) throws IOException {
    Preconditions.checkArgument(pos >= mBegin);
    Preconditions.checkArgument(mData.size() > 0);
    List<ByteBuf> res = new ArrayList<>();
    if (pos >= mEnd) {
      return null;
    }
    long newBegin = mBegin;

    Iterator<ByteBuf> iter = mData.iterator();
    ByteBuf current = iter.next();

    //skip ByteBuf needing not to read.
    while (pos > (newBegin + current.capacity())) {
      newBegin += current.capacity();
      current = iter.next();
    }

    int leftToRead = (int) Math.min(mEnd - pos, len);
    int readedLen = 0;
    // skip the first bytebuf reduntant byte len;
    if (pos > newBegin) {
      int currentLeftCanReadLen = current.capacity() - (int) (pos - newBegin);
      int readLen = Math.min(currentLeftCanReadLen, leftToRead);

      res.add(current.slice((int) (pos - newBegin), readLen).retain());

      leftToRead -= readLen;
      readedLen += readLen;
      if (iter.hasNext()) {
        current = iter.next();
      }
    }

    while (leftToRead > 0) {
      int readLen = Math.min(current.capacity(), leftToRead);

      res.add(current.slice(0, readLen).retain());

      leftToRead -= readLen;
      readedLen += readLen;
      if (iter.hasNext()) {
        current = iter.next();
      } else {
        break;
      }
    }
    return res;
  }

  /**
   * @param b     the result byte array
   * @param off   the begin position of byte b
   * @param begin the begin position of file
   * @param len   the end position of file
   * @return
   */
  public int positionedRead(byte[] b, int off, long begin, int len) {
    Preconditions.checkArgument(begin >= mBegin);
    Preconditions.checkArgument(mData.size() > 0);
    if (begin >= mEnd) {
      return -1;
    }
    long newBegin = mBegin;

    Iterator<ByteBuf> iter = mData.iterator();
    ByteBuf current = iter.next();

    //skip ByteBuf needing not to read.
    while (begin > (newBegin + current.capacity())) {
      newBegin += current.capacity();
      current = iter.next();
    }

    int leftToRead = (int) Math.min(mEnd - begin, len);
    int hitLen = 0;
    int readedLen = 0;
    // skip the first bytebuf reduntant byte len;
    if (begin > newBegin) {
      int currentLeftCanReadLen = current.capacity() - (int) (begin - newBegin);
      int readLen = Math.min(currentLeftCanReadLen, leftToRead);
      long startTick = System.currentTimeMillis();
      current.getBytes((int) (begin - newBegin), b, off, readLen);
      ClientCacheStatistics.INSTANCE.copyBufferTime += (System.currentTimeMillis() - startTick);
      leftToRead -= readLen;
      if (FixLengthReadNote.isIsUsingNote()) {
        hitLen += FixLengthReadNote.realHitLen(begin, readLen);
      } else {
        hitLen += readLen;
      }
      readedLen += readLen;
      if (iter.hasNext()) {
        current = iter.next();
        newBegin += current.capacity();
      }
    }

    while (leftToRead > 0) {
      int readLen = Math.min(current.capacity(), leftToRead);
      long startTick = System.currentTimeMillis();
      current.getBytes(0, b, off + readedLen, readLen);
      ClientCacheStatistics.INSTANCE.copyBufferTime += (System.currentTimeMillis() - startTick);
      leftToRead -= readLen;
      if (FixLengthReadNote.isIsUsingNote()) {
        hitLen += FixLengthReadNote.realHitLen(newBegin, readLen);
      } else {
        hitLen += readLen;
      }
      readedLen += readLen;
      if (iter.hasNext()) {
        current = iter.next();
        newBegin += current.capacity();
      } else {
        break;
      }
    }


    HitRatioMetric.INSTANCE.hitSize += hitLen;
    return readedLen;
  }

  @Override
  public boolean isFinish() {
    return true;
  }

  @Override
  public String toString() {
    return "finish begin: " + mBegin + "end: " + mEnd + " file id : "+ mFileId;
  }

  private void reset(int newBegin, int newEnd) {
    mBegin = newBegin;
    mEnd = newEnd;
  }

  public List<ByteBuf> getAllData() {
    return mData;
  }


  /**
   * Needed by hash set judging two element equals
   */
  @Override
  public int hashCode() {
    return (int) ((this.mEnd * 31 + this.mBegin) * 31 + this.mFileId) * 31;
  }


  /**
   * Needed by hash set judging two element equals
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof CacheInternalUnit) {
      CacheInternalUnit tobj = (CacheInternalUnit) obj;
      return this.getFileId() == tobj.getFileId() && this.mBegin == tobj.getBegin() && this.mEnd == tobj.getEnd();
    }
    return false;
  }

  public int compareTo(CacheInternalUnit node) {
    if (node.getBegin() >= this.mEnd) {
      return -1;
    } else if (node.getEnd() <= this.mBegin) {
      return 1;
    }
    return 0;
  }

  private int deletePart(long begin, long newbegin, int lastRemain, List<CacheInternalUnit> res) {
    int i = 0;
    if (lastRemain > 0 && lastRemain <= newbegin - begin) {
      begin += lastRemain;
      ByteBuf needDelete = mData.remove(0);
      ClientCacheContext.release(needDelete);
    } else if (lastRemain > newbegin - begin) {
      return lastRemain - (int) (newbegin - begin);
    }
    while (newbegin - begin > 0) {
      ByteBuf buf = mData.get(i);
      begin += buf.capacity();
      if (begin > newbegin) {
        return (int) (begin - newbegin);
      } else {
        ByteBuf buf1 = mData.remove(i);
        ClientCacheContext.release(buf1);
      }
    }
    return 0;
  }

  public void printtest() {
    for (ByteBuf buf : mData) {
      for (int i = 0; i < buf.capacity(); i++) {
        System.out.print(buf.getByte(i) + " ");
      }
      System.out.println();
    }
  }

  private int generateSubCacheUnit(long newBegin, long newEnd, int lastRemain,
                                   List<CacheInternalUnit> res) {
    CacheInternalUnit newUnit = new CacheInternalUnit(newBegin, newEnd, mFileId);
    try {
      long start = newBegin;
      if (lastRemain > 0) {
        if (lastRemain <= newEnd - newBegin) {
          ByteBuf buf = mData.get(0);
          buf.readerIndex(buf.capacity() - lastRemain);
          ByteBuf newBuf = buf.slice();
          //ByteBuf newBuf = buf.copy(buf.readerIndex(), buf.readableBytes());
          mData.remove(0);
          //ClientCacheContext.mAllocator.release(buf);

          //buf.clear();
          newUnit.mData.add(newBuf);
          start += lastRemain;
        } else {
          ByteBuf buf = mData.get(0);
          ByteBuf newBuf = buf.slice(buf.capacity() - lastRemain, (int) (newEnd - newBegin));
          //ByteBuf newBuf = buf.copy(buf.capacity() - lastRemain,(int)
          // (newEnd - newBegin));
          // ReferenceCountUtil.release(buf);
          newUnit.mData.add(newBuf);
          return lastRemain - (int) (newEnd - newBegin);
        }
      }
      while (start < newEnd) {
        ByteBuf tmp = mData.get(0);
        int len = tmp.capacity();
        if (start + len <= newEnd) {
          newUnit.mData.add(tmp);
          start += len;
          mData.remove(0);
        } else {
          ByteBuf newBuf = tmp.slice(0, (int) (newEnd - start));
          //ByteBuf newBuf = tmp.copy(0, (int) (newEnd - start));
          newUnit.mData.add(newBuf);
          //ReferenceCountUtil.release(tmp);
          return len - (int) (newEnd - start);
        }
      }
      return 0;
    } finally {
      this.before.after = newUnit;
      newUnit.before = this.before;
      newUnit.after = this;
      this.before = newUnit;
      if (mTmpSplitQueue != null) {
        newUnit.accessRecord.addAll(mTmpSplitQueue.poll());
      }
      res.add(newUnit);
    }
  }

  public List<CacheInternalUnit> split(Queue<LongPair> tmpQueue) {
    mDeleteCacheSize = 0;
    long lastEnd = mBegin;
    int lastByteRemain = 0;
    List<CacheInternalUnit> res = new ArrayList<>();
    while (!tmpQueue.isEmpty()) {
      LongPair currentPart = tmpQueue.poll();
      long begin = currentPart.getKey();
      long end = currentPart.getValue();
      if (begin == mBegin && end == mEnd) {
        res.add(this);
        return res;
      }
      if (begin < 0) continue;
      if (begin != lastEnd) {
        lastByteRemain = deletePart(lastEnd, begin, lastByteRemain, res);
        mDeleteCacheSize += (begin - lastEnd);
        if (ClientCacheContext.INSTANCE.mUseGhostCache && lastEnd >= 0 && begin > lastEnd) {
          ClientCacheContext.INSTANCE.getGhostCache().add(new BaseCacheUnit(lastEnd, begin, mFileId));
        }
      }
      lastByteRemain = generateSubCacheUnit(begin, end, lastByteRemain, res);
      lastEnd = end;
    }
    if (lastEnd != mEnd) {
      deletePart(lastEnd, mEnd, lastByteRemain, res);
      mDeleteCacheSize += (mEnd - lastEnd);
      if (ClientCacheContext.INSTANCE.mUseGhostCache && lastEnd >= 0 && mEnd > lastEnd) {
        ClientCacheContext.INSTANCE.getGhostCache().add(new BaseCacheUnit(lastEnd, mEnd, mFileId));
      }
    }
    return res;
  }

  private void generateSubCacheUnitTest(long newBegin, long newEnd, LinkedFileBucket bucket, List<CacheInternalUnit> res) {
    CacheInternalUnit newUnit = new CacheInternalUnit(newBegin, newEnd, mFileId);

    this.before.after = newUnit;
    newUnit.before = this.before;
    newUnit.after = this;
    this.before = newUnit;
    bucket.add(newUnit);
    if (mTmpSplitQueue != null) {
      newUnit.accessRecord.addAll(mTmpSplitQueue.poll());
    }
    res.add(newUnit);

  }


  public List<CacheInternalUnit> splitTest(Queue<LongPair> tmpQueue, LinkedFileBucket bucket) {
    mDeleteCacheSize = 0;
    long lastEnd = mBegin;
    List<CacheInternalUnit> res = new ArrayList<>();
    while (!tmpQueue.isEmpty()) {
      LongPair currentPart = tmpQueue.poll();
      long begin = currentPart.getKey();
      long end = currentPart.getValue();
      if (begin == mBegin && end == mEnd) {
        res.add(this);
        return res;
      }
      if (begin < 0) continue;
      if (begin != lastEnd) {
        mDeleteCacheSize += (begin - lastEnd);
      }
      generateSubCacheUnitTest(begin, end, bucket, res);
      lastEnd = end;
    }
    if (lastEnd != mEnd) {
      mDeleteCacheSize += (mEnd - lastEnd);
    }
    return res;
  }

  public void clearTreeIndex() {
    this.left = this.right = this.parent = null;
  }

  public void clearData() {
    for (ByteBuf b : mData) {
      ClientCacheContext.release(b);
      //  ReferenceCountUtil.release(b);
      //b.clear();
      //b = null;
    }
    //ClientCacheContext.mAllocator.release(mData);
  }

  public void resetData() {
    for (ByteBuf b : mData) {
      //ClientCacheContext.release(b);
      ReferenceCountUtil.release(b);
      //b.clear();
      //b = null;
    }
  }
}
