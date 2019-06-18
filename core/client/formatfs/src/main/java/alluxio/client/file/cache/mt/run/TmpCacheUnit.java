package alluxio.client.file.cache.mt.run;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.List;

public class TmpCacheUnit implements Comparable  {
  private long mBegin, mEnd, mFileId;
  public long mClientIndex;
  public double mCost =0;
  private int mAccessTime = 0;
  private int mAccessInterval = 0;

  public int getmAccessTime() {
    return mAccessTime;
  }

  public TmpCacheUnit setmAccessTime(int mAccessTime) {
    this.mAccessTime = mAccessTime;
    return this;
  }

  public int getmAccessInterval() {
    return mAccessInterval;
  }
  public void setmAccessInterval(int i ) {
    mAccessInterval = i;
  }

  public TmpCacheUnit(long fileId, long begin, long end) {
    mBegin = begin;
    mEnd = end;
    mFileId = fileId;
  }

  public List<ByteBuf> get(long pos, long len) throws IOException {
    return null;
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

  @Override
  public String toString() {
    return "empty unit begin: " + mBegin + "end: " + mEnd
            + " file id :" + mFileId + " access time " + mAccessTime + " accessInterval " + mAccessInterval;
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
    if (obj instanceof TmpCacheUnit) {
      TmpCacheUnit tobj = (TmpCacheUnit) obj;
      return this.getFileId() == tobj.getFileId() && this.mBegin == tobj.getBegin() && this.mEnd == tobj.getEnd();
    }
    return false;
  }

  @Override
  public int compareTo(Object o) {
    TmpCacheUnit u = (TmpCacheUnit)o;
    if (u.mAccessTime != mAccessTime) {
      return mAccessTime - u.mAccessTime;
    } else {
      return mAccessInterval - u.getmAccessInterval();
    }
  }
}