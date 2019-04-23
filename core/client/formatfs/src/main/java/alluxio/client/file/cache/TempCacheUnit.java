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

import alluxio.client.file.FileInStream;
import alluxio.client.file.cache.struct.LinkNode;
import alluxio.client.file.cache.struct.LongPair;

import alluxio.network.netty.NettyRPC;
import com.google.common.base.Preconditions;
import com.sun.prism.shader.Solid_TextureYV12_AlphaTest_Loader;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;
import sun.misc.Cache;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static alluxio.client.file.cache.ClientCacheContext.useNettyMemoryUtils;

public class TempCacheUnit extends LinkNode<TempCacheUnit> implements CacheUnit {
  long mFileId;
  private long mBegin;
  private long mEnd;
  public Deque<CacheInternalUnit> mCacheConsumer = new LinkedList<>();
  //private CompositeByteBuf mData;
  public List<ByteBuf> mData = new LinkedList<>();
  CacheInternalUnit mBefore;
  CacheInternalUnit mAfter;
  public FileInStream in;
  private long mSize;
  private long mNewCacheSize;
  private long mRealReadSize;
  private double mHitvalue;
  private LockTask mLockTask;

  public TreeSet<BaseCacheUnit> mTmpAccessRecord = new TreeSet<>(new Comparator<CacheUnit>() {
    @Override
    public int compare(CacheUnit o1, CacheUnit o2) {
      return (int) (o1.getBegin() - o2.getBegin());
    }
  });

  public LongPair mCacheIndex = null;

  public Queue<CacheInternalUnit> deleteQueue = new LinkedList<>();

  public long newSize;

  public long getSize() {
    return mSize;
  }

  public long getNewCacheSize() {
    return mNewCacheSize;
  }

  public double getHitValue() {
    return mHitvalue;
  }

  public void setCurrentHitVal(double hit) {
    mHitvalue = hit;
  }


  @Override
  public long getFileId() {
    return mFileId;
  }

  @Override
  public long getBegin() {
    return mBegin;
  }

  public TempCacheUnit() {
  }

  public void setLockTask(LockTask task) {
    mLockTask = task;
  }

  public LockTask getLockTask() {
    return mLockTask;
  }

  public void init(long begin, long end, long fileId) {
    mBegin = begin;
    mEnd = end;
    mFileId = fileId;
    mSize = mEnd - mBegin;
    mNewCacheSize = 0;
  }

  public TempCacheUnit(long begin, long end, long fileId) {
    mBegin = begin;
    mEnd = end;
    mFileId = fileId;
    mSize = mEnd - mBegin;
    mNewCacheSize = 0;
  }

  public boolean isInternal() {
    if (mCacheConsumer.size() != 1) return false;
    CacheInternalUnit unit = mCacheConsumer.peek();
    return unit.getFileId() == mFileId && unit.getBegin() == mBegin && unit.getEnd() == mEnd;
  }


  public void compareCacheIndex(BaseCacheUnit unit) {
    if (mCacheIndex == null) {
      mCacheIndex = new LongPair(unit.getBegin(), unit.getEnd());
    } else {
      if (mCacheIndex.getValue() >= unit.getBegin()) {
        mCacheIndex.setValue(unit.getEnd());
      } else {

      }
    }
  }

  public void setInStream(FileInStream i) {
    in = i;
  }

  public void resetEnd(long end) {
    mEnd = end;
    mSize = mEnd - mBegin;
  }

  public void resetBegin(long begin) {
    mBegin = begin;
    mSize = mEnd - mBegin;
  }

  public long getEnd() {
    return mEnd;
  }

  public long consumeResource(boolean isCache) {
    CacheInternalUnit unit = mCacheConsumer.poll();
    if (isCache) {
      List<ByteBuf> tmp = unit.getAllData();
      mData.addAll(tmp);
      mTmpAccessRecord.addAll(unit.accessRecord);
      deleteQueue.add(unit);
    }
    return unit.getSize();
  }


  /**
   * Read from file or cache, don't cache read data to cache List
   */
  public int read(byte[] b, int off, int len) throws IOException {
    long pos = mBegin;
    long end = Math.min(mEnd, mBegin + len);
    int leftToRead = (int) (end - mBegin);
    mRealReadSize = leftToRead;
    int distPos = off;
    if (hasResource()) {
      CacheInternalUnit current = getResource();
      boolean beyondCacheList = false;
      int readLength;
      while (pos <= end) {
        //read from cache
        if (pos >= current.getBegin()) {
          readLength = current.positionedRead(b, distPos, pos, leftToRead);
          if (hasResource()) {
            current = getResource();
          } else {
            beyondCacheList = true;
          }
        }
        //read from File, the need byte[] is before the current CacheUnit
        else {
          int needreadLen;
          if (!beyondCacheList) {
            needreadLen = (int) (current.getBegin() - pos);
          } else {
            needreadLen = (int) (end - pos);
          }
          readLength = ((FileInStreamWithCache)in).innerRead(b, distPos, needreadLen);
        }
        // change read variable
        if (readLength != -1) {
          pos += readLength;
          distPos += readLength;
          leftToRead -= readLength;
        }
      }
      return distPos - off;
    } else {
      return ((FileInStreamWithCache)in).innerRead(b, off, leftToRead);
    }
  }

  /**
   * Read from file or cache, cache data to cache List
   */
  public int lazyRead(byte[] b, int off, int len, long readPos, boolean isCache) throws IOException {

      boolean positionedRead = false;
      if (readPos != in.getPos()) {
        positionedRead = true;
      }
      long pos = readPos;
      long end = Math.min(mEnd, readPos + len);
      int leftToRead = (int) (end - readPos);
      mRealReadSize = leftToRead;
      int distPos = off;

      if (hasResource()) {

        CacheInternalUnit current = getResource();
        boolean beyondCacheList = false;
        int readLength = -1;

        while (pos < end) {
          //read from cache
          if (current != null && pos >= current.getBegin()) {
            readLength = current.positionedRead(b, distPos, pos, leftToRead);

            if (readLength != -1 && !positionedRead) {
              in.skip(readLength);
            }
            consumeResource(isCache);
            if (hasResource()) {
              current = getResource();
            } else {
              beyondCacheList = true;
              current = null;
            }
          }
          //read from File, the need byte[] is before the current CacheUnit
          else {
            int needreadLen;
            needreadLen = (int) (end - pos);
            if (!beyondCacheList) {
              needreadLen = Math.min((int) (current.getBegin() - pos), needreadLen);
            }
            if (!positionedRead) {
              readLength = ((FileInStreamWithCache) in).innerRead(b, distPos, needreadLen);
            } else {
              readLength = ((FileInStreamWithCache) in).innerPositionRead(pos, b, distPos, needreadLen);
            }
            if (readLength != -1) {
              if (isCache) addCache(b, distPos, readLength);
              mNewCacheSize += readLength;
            }
          }
          // change read variable
          if (readLength != -1) {
            pos += readLength;
            distPos += readLength;
            leftToRead -= readLength;
          }
        }
        return distPos - off;
      } else {

        int readLength;
        if (!positionedRead) {

          readLength = ((FileInStreamWithCache) in).innerRead(b, off, leftToRead);

        } else {

          readLength = ((FileInStreamWithCache) in).innerPositionRead(pos, b, off, leftToRead);

        }

        if (readLength > 0) {
          if (isCache) addCache(b, off, readLength);
          mNewCacheSize += readLength;
        }

        return readLength;
      }

  }

  public void addResource(CacheInternalUnit unit) {
    if (mCacheConsumer.isEmpty()) {
      mBefore = unit.before;
    }
    mAfter = unit.after;
    mCacheConsumer.add(unit);
  }

  public void addResourceReverse(CacheInternalUnit unit) {
    if (mCacheConsumer.isEmpty()) {
      mAfter = unit.after;
    }
    mBefore = unit.before;
    mCacheConsumer.addFirst(unit);
  }


  public int addCacheByNettyPool(FileInStream in, int off, int len, List<ByteBuf> res) throws IOException {
    int cacheSize = ClientCacheContext.CACHE_SIZE;
    int readLen = 0;
    boolean allowStream = off == in.getPos();
    int len1;
    for (int i = off; i < len + off; i += cacheSize) {
      if (len + off - i > cacheSize) {
        ByteBuf tmp = PooledByteBufAllocator.DEFAULT.heapBuffer(cacheSize);
        //byte[] b = tmp.array();
        byte[] b = new byte[cacheSize];
        if (!allowStream) {
          len1 = in.positionedRead(i, b, 0, cacheSize);
        } else {
          len1 = in.read(b, 0, cacheSize);
        }
        mData.add(tmp);
        tmp.writerIndex(len1);
        readLen += len1;
      } else {
        ByteBuf tmp = PooledByteBufAllocator.DEFAULT.heapBuffer(len + off - i);
        byte[] b = tmp.array();
        if (!allowStream) {
          len1 = in.positionedRead(i, b, 0, cacheSize);
        } else {
          len1 = in.read(b, 0, cacheSize);
        }
        mData.add(tmp);
        tmp.writerIndex(len1);
        readLen += len1;
      }
      // off += readLen;
    }
    return readLen;
  }

  private int addCache(FileInStream in, int off, int len, List<ByteBuf> res) throws
    IOException {
    if (useNettyMemoryUtils) {
      return addCacheByNettyPool(in, off, len, res);
    } else {
      return addCacheByCachePool(in, off, len, res);
    }
  }

  private int addCacheByCachePool(FileInStream in, int off, int len, List<ByteBuf> data)
    throws IOException {
    List<ByteBuf> tmp = ClientCacheContext.mAllocator.allocate(len);
    boolean allowStream = off == in.getPos();
    int readLen = 0;
    for (ByteBuf tmp1 : tmp) {
      int res;
      if (!allowStream) {
        byte tmpBytes [] = new byte[tmp1.capacity()];
        res = in.positionedRead(off, tmpBytes, 0, tmp1.capacity());
        tmp1.writeBytes(tmpBytes);
        throw new RuntimeException("this should never happen");
      } else {
        //res = in.innerRead(b, 0, tmp1.capacity());
        res = tmp1.writeBytes(in, tmp1.capacity());
      }
      if (data == null) {
        mData.add(tmp1);
      } else {
        data.add(tmp1.retain());
      }

      off += res;
      tmp1.writerIndex(tmp1.capacity());
      readLen += res;
      if (off != in.getPos()) {
        System.out.println(off + " . " + in.getPos() + " . " + allowStream);
        throw new RuntimeException();
      }
    }
    Preconditions.checkArgument(readLen == len && tmp.size() > 0);
    return readLen;
  }


  @Override
  public List<ByteBuf> get(long pos, long len) throws IOException {
    long end = Math.min(mEnd, pos + len);
    boolean needDeleteLast = false;
    boolean needDeleteFirst = false;
    if (end != mEnd) needDeleteLast = true;
    if (mBegin != pos) needDeleteFirst = true;
    int leftToRead = (int) (end - pos);
    mRealReadSize = leftToRead;
    if (needDeleteFirst) mBegin = pos;
    if (needDeleteLast) mEnd = end;
    mSize = mEnd - mBegin;
    List<ByteBuf> res = new ArrayList<>();
    if (hasResource()) {
      CacheInternalUnit current = getResource();
      boolean beyondCacheList = false;
      while (pos < end) {
        long readLength = -1;
        //read from cache
        if (current != null && pos >= current.getBegin()) {
          if ((needDeleteFirst && pos > current.getBegin()) || (needDeleteLast && pos + current
            .getSize() > end)) {
            mCacheConsumer.poll();
            List<ByteBuf> tmpRes = current.get(pos, Math.min(current.getEnd(), end) - pos);
            res.addAll(tmpRes);

          } else {
            CacheInternalUnit unit = mCacheConsumer.poll();
            List<ByteBuf> tmp = unit.getAllData();
            for (ByteBuf t : tmp) {
              t.retain();
            }
            mData.addAll(tmp);
            readLength = unit.getSize();
          }
          if (hasResource()) {
            current = getResource();
          } else {
            beyondCacheList = true;
            current = null;
          }
        } else {
          int needreadLen = (int) (end - pos);
          if (!beyondCacheList) {
            needreadLen = Math.min((int) (current.getBegin() - pos), needreadLen);
          }
          readLength = addCache(in, (int) pos, needreadLen, res);
        }
        // change read variable
        if (readLength != -1) {
          pos += readLength;
          leftToRead -= readLength;
        }
      }
      return res;
    } else {
      addCache(in, (int) pos, leftToRead, res);
      return res;
    }
  }

  public int cache(long pos, int len, FileCacheUnit unit) throws
    IOException {
    boolean positionedRead = false;
    if (pos != in.getPos()) {
      positionedRead = true;
    }
    long end = Math.min(mEnd, pos + len);
    boolean needDeleteLast = false;
    boolean needDeleteFirst = false;
    if (end != mEnd) needDeleteLast = true;
    if (mBegin != pos) needDeleteFirst = true;
    int leftToRead = (int) (end - pos);
    mRealReadSize = leftToRead;
    if (needDeleteFirst) mBegin = pos;
    if (needDeleteLast) mEnd = end;
    mSize = mEnd - mBegin;
    if (hasResource()) {
      CacheInternalUnit current = getResource();
      boolean beyondCacheList = false;
      while (pos < end) {
        long readLength = -1;
        //read from cache
        if (current != null && pos >= current.getBegin()) {
          if ((needDeleteFirst && pos > current.getBegin()) || (needDeleteLast && pos + current
            .getSize() > end)) {
            mCacheConsumer.poll();
            Queue<LongPair> q = new LinkedList<>();
            q.add(new LongPair(pos, Math.min(current.getEnd(), end)));
            List<CacheInternalUnit> first = current.split(q);

            CacheInternalUnit newFirstUnit = first.get(0);
            //delete original cache unit

            unit.mBuckets.delete(current);
            unit.getCacheList().delete(current);
           // current.resetData();
            current = null;
            unit.mBuckets.add(first.get(0));

            readLength =  newFirstUnit.getSize();
            List<ByteBuf> tmp = newFirstUnit.getAllData();
            mData.addAll(tmp);
            mTmpAccessRecord.addAll(newFirstUnit.accessRecord);
            deleteQueue.add(newFirstUnit);
            //mLockTask.deleteUnlock();
          } else {
            readLength = consumeResource(true);
          }
          if (hasResource()) {
            current = getResource();
          } else {
            beyondCacheList = true;
            current = null;
          }
          if (!positionedRead && readLength > 0) {
            in.skip(readLength);
          }
        } else {
          int needreadLen = (int) (end - pos);
          if (!beyondCacheList) {
            needreadLen = Math.min((int) (current.getBegin() - pos), needreadLen);
          }
          readLength = addCache(in, (int) pos, needreadLen, null);
          if (readLength != -1) {
            mNewCacheSize += readLength;
          }
        }
        // change read variable
        if (readLength != -1) {
          pos += readLength;
          leftToRead -= readLength;
        }
      }
      return (int) mNewCacheSize;
    } else {
      int readLength = addCache(in, (int) pos, leftToRead, null);
      if (readLength != -1) {
        mNewCacheSize += readLength;
      }
      return (int) mNewCacheSize;
    }
  }

  public void addCache(byte[] b, int off, int len) {
    if (useNettyMemoryUtils) {
      addCacheByNettyPool(b, off, len);
    } else {
      addCacheByCachePool(b, off, len);
    }
  }

  private void addCacheByCachePool(byte[] b, int off, int len) {
    List<ByteBuf> tmp = ClientCacheContext.mAllocator.allocate(len);
    int pos = off;
    for (ByteBuf tmp1 : tmp) {
      tmp1.writeBytes(b, pos, tmp1.capacity());
      pos += tmp1.capacity();
      mData.add(tmp1);
    }
  }

  private void addCacheByNettyPool(byte[] b, int off, int len) {
    int cacheSize = ClientCacheContext.CACHE_SIZE;
    for (int i = off; i < len + off; i += cacheSize) {
      if (len + off - i > cacheSize) {
        ByteBuf tmp = PooledByteBufAllocator.DEFAULT.heapBuffer(cacheSize);
        tmp.writeBytes(b, i, cacheSize);
        mData.add(tmp);
      } else {
        ByteBuf tmp = PooledByteBufAllocator.DEFAULT.heapBuffer(len + off - i);
        tmp.writeBytes(b, i, len + off - i);
        mData.add(tmp);
      }
    }
  }

  /**
   * This function must called after lazyRead() function called.
   *
   * @return new Cache Unit to put in cache space.
   */
  public CacheInternalUnit convert() {
    while (!mCacheConsumer.isEmpty()) {
      consumeResource(true);
    }

    // the tmp unit become cache unit to put into cache space, so, the data ref need
    // to add 1;
    for (ByteBuf buf : mData) {
      buf.retain();
    }
    CacheInternalUnit result = new CacheInternalUnit(mBegin, mEnd, mFileId, mData);
    result.before = this.mBefore;
    result.after = this.mAfter;
    result.accessRecord.addAll(mTmpAccessRecord);
    return result;
  }

  public CacheInternalUnit convertType() {
    CacheInternalUnit result = new CacheInternalUnit(mBegin, mEnd, mFileId, null);
    return result;
  }

  public CacheInternalUnit getResource() {
    return mCacheConsumer.peek();
  }

  public boolean hasResource() {
    return !mCacheConsumer.isEmpty();
  }

  @Override
  public boolean isFinish() {
    return false;
  }

  @Override
  public String toString() {
    return "unfinish begin: " + mBegin + "end: " + mEnd;
  }

  @Override
  public int hashCode() {
    return (int) ((this.mEnd * 31 + this.mBegin) * 31 + this.mFileId) * 31;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TempCacheUnit) {
      TempCacheUnit tobj = (TempCacheUnit) obj;
      return this.mBegin == tobj.getBegin() && this.mEnd == tobj.getEnd();
    }
    return false;
  }

  public int compareTo(TempCacheUnit node) {
    if (node.getBegin() >= this.mEnd) {
      return -1;
    } else if (node.getEnd() <= this.mBegin) {
      return 0;
    }
    return 0;
  }

}
