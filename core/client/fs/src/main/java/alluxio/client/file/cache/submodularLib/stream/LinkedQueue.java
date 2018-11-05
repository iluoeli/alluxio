package alluxio.client.file.cache.submodularLib.stream;


import alluxio.client.file.cache.BaseCacheUnit;
import alluxio.client.file.cache.CacheInternalUnit;
import alluxio.client.file.cache.CacheUnit;
import alluxio.client.file.cache.struct.DoubleLinkedList;

import java.util.*;

public class LinkedQueue extends LinkedList<CacheUnit> {
  private DoubleLinkedList<CacheInternalUnit> mCacheList;
  private Iterator<BaseCacheUnit> mIter;
  private CacheInternalUnit mCurrent = null;

  public LinkedQueue(DoubleLinkedList<CacheInternalUnit> list) {
    mCacheList = list;
    mCurrent = mCacheList.head.after;
    mIter = mCurrent.accessRecord.iterator();
  }

  public boolean isEmpty() {
    if (mCurrent == null) return true;
    if (!mIter.hasNext() && mCurrent.after == null) {
      mCacheList.delete(mCurrent);
      return true;
    }
    return false;
  }

  public CacheUnit poll() {
    if (mIter.hasNext()) {
      return mIter.next();
    } else {
      CacheInternalUnit pre = mCurrent;
      mCurrent = mCurrent.after;
      mCacheList.delete(pre);
      if (mCurrent != null) {
        mIter = mCurrent.accessRecord.iterator();
        return mIter.next();
      } else {
        return null;
      }
    }
  }
}
