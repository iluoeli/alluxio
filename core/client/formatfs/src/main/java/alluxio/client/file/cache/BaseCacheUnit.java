package alluxio.client.file.cache;

import alluxio.client.file.cache.struct.LinkNode;
import io.netty.buffer.ByteBuf;
import org.apache.hadoop.io.serializer.Serialization;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class BaseCacheUnit extends LinkNode<BaseCacheUnit> implements CacheUnit, Serializable{
  private long mBegin, mEnd, mFileId;
  private double currentHitVal;
  private long mPureIncrease;
  public int mIndex;
  public double mCost;

  public BaseCacheUnit(long fileId, long begin, long end) {
    mBegin = begin;
    mEnd = end;
    mFileId = fileId;
    currentHitVal = 1;
    mPureIncrease = 0;
  }

  public List<ByteBuf> get(long pos, long len) throws IOException {
    return null;
  }


  public void addReadLockIndex(int index) {}

  public long getPureIncrease() {
    return mPureIncrease;
  }

  public void setPureIncrease(long increase) {
    mPureIncrease = increase;
  }

  public long getIncrease() {
    return mPureIncrease;
  }

  public void setCurrentHitVal(double val) {
    currentHitVal = val;
  }

  public double getHitValue() {
    return currentHitVal;
  }

  public boolean isFinish() {
    return false;
  }

  public long getBegin() {
    return mBegin;
  }

  public long getEnd() {
    return mEnd;
  }

  public long getFileId() {
    return mFileId;
  }

  public long getSize() {
    return mEnd - mBegin;
  }


  public boolean isCoincience(CacheUnit u2) {
    try {
      if (getFileId() != u2.getFileId()) {
        return false;
      }
      if (mBegin == u2.getBegin() && mEnd == u2.getEnd()) {
        return false;
      }
      if (mBegin <= u2.getBegin() && mEnd > u2.getBegin()) {
        return true;
      }
      if (mBegin < u2.getEnd()) {
        return true;
      }
      return false;
    } finally {
      //	ClientCacheContext.INSTANCE.compute += System.currentTimeMillis() -
      // begin;
    }
  }

  @Override
  public String toString() {
    return "empty unit begin: " + mBegin + "end: " + mEnd;
  }

  public int compareTo(BaseCacheUnit node) {
      return (int)(mCost - node.mCost);

  }

  public void setLockTask(LockTask task){}

  public LockTask getLockTask(){return null;}
}
