package alluxio.client.file.cache;

import alluxio.client.file.cache.struct.LinkNode;
import alluxio.client.file.cache.struct.RBTree;

import java.util.Iterator;

import static alluxio.client.file.cache.ClientCacheContext.fileId;

public class LinkedFileBucket {
  public LinkBucket[] mCacheIndex0;
  /**
   * The num of fileBucket of one file
   */
  private int BUCKET_LENGTH = ClientCacheContext.INSTANCE.BUCKET_LENGTH;
  /**
   * The length of one bucket.
   */
  private long mBucketLength;
  private final boolean IS_REVERT = ClientCacheContext.INSTANCE.REVERSE;
  private final long mFileId;
  private final ClientCacheContext.LockManager mLockManager;

  public LinkedFileBucket(long fileLength, long fileId, ClientCacheContext.LockManager lockManager) {
    mBucketLength = (long) (fileLength / (double) BUCKET_LENGTH);
    if (fileLength % BUCKET_LENGTH != 0) {
      BUCKET_LENGTH++;
    }
    mFileId = fileId;
    mCacheIndex0 = new LinkBucket[BUCKET_LENGTH];
    mLockManager = lockManager;
  }

  public int getIndex(long begin, long end) {
    int index;
    if (IS_REVERT) {
      index = (int) (end / mBucketLength);
      index = end % mBucketLength == 0 ? index - 1 : index;
    } else {
      index = (int) (begin / mBucketLength);
    }
    return index;
  }

  public void add(CacheInternalUnit unit) {
    int index = getIndex(unit.getBegin(), unit.getEnd());
    LinkBucket bucket = mCacheIndex0[index];
    unit.initBucketIndex(index);
    if (bucket == null) {
      bucket = new RBTreeBucket(index);
      mCacheIndex0[index] = bucket;
    }
    bucket.addNew(unit);
  }

  public void delete(CacheInternalUnit unit) {
    int index = getIndex(unit.getBegin(), unit.getEnd());
    mCacheIndex0[index].delete(unit);
  }

  public CacheUnit find(long begin, long end) {

    int index = getIndex(begin, end);
    LinkBucket bucket = mCacheIndex0[index];

    if (bucket == null || bucket.mUnitNum == 0) {
      mLockManager.initBucketLock(mFileId, index);
      int left, right;
      left = right = index;
      while (true) {
        left--;
        right++;
        if (left < 0) {
          mLockManager.initBucketLock(mFileId, 0);
          return ClientCacheContext.INSTANCE.getKeyFromBegin(begin, end, mFileId);
        } else if (right >= BUCKET_LENGTH) {
          mLockManager.initBucketLock(mFileId, BUCKET_LENGTH - 1);
          return ClientCacheContext.INSTANCE.getKeyByReverse2(begin, end, mFileId, BUCKET_LENGTH - 1);
        } else if (mCacheIndex0[right] != null && mCacheIndex0[right].mUnitNum > 0) {
          CacheInternalUnit before = mCacheIndex0[right].mStart.before;
          Iterator<CacheInternalUnit> iter = new TmpIterator<>(mCacheIndex0[right].mStart, null);
          CacheUnit unit = ClientCacheContext.INSTANCE.getKey(begin, end, mFileId, iter, right);
          if (unit.isFinish()) return unit;
          TempCacheUnit tmp = (TempCacheUnit) unit;
          if (before != null && tmp.getBegin() < before.getEnd()) {
            return ClientCacheContext.INSTANCE.handleRightCoincidence(tmp, before, true, index);
          } else {
            return tmp;
          }
        } else if (mCacheIndex0[left] != null && mCacheIndex0[left].mUnitNum > 0) {
          CacheInternalUnit after = mCacheIndex0[left].mEnd.after;
          PreviousIterator<CacheInternalUnit> iter = new TmpIterator<>(null, mCacheIndex0[left].mEnd);
          CacheUnit unit = ClientCacheContext.INSTANCE.getKeyByReverse(begin, end, mFileId, iter, left);
          if (unit.isFinish()) return unit;
          TempCacheUnit tmp = (TempCacheUnit) unit;
          if (after != null && tmp.getEnd() > after.getBegin()) {
            return ClientCacheContext.INSTANCE.handleLeftCoincidence(after, tmp, true, index);
          } else {
            return tmp;
          }
        }
      }
    }
    return bucket.find(begin, end);
  }

  public void print() {
    for (LinkBucket bucket : mCacheIndex0) {
      if (bucket != null && bucket.mUnitNum != 0)
        System.out.println("Start : " + bucket.mStart.toString() + " end : " + bucket.mEnd.toString() + bucket.mUnitNum);
    }
  }



  /*
  private class SkipListBucket extends LinkBucket {
    SkipListBucket(long begin) {
      super(begin);
    }

    @Override
    public void convert(CacheInternalUnit unit, int num) {

    }

    @Override
    public void addToIndex(CacheInternalUnit unit) {
    }

    @Override
    public CacheUnit findByIndex(long begin, long end) {
      return null;

    }

  }*/

  public class RBTreeBucket extends LinkBucket {
    public RBTree<CacheInternalUnit> mCacheIndex1;

    RBTreeBucket(int index) {
      super(index);
      mCacheIndex1 = new RBTree<>();
    }

    @Override
    public void convert(CacheInternalUnit unit, int num) {
      for (int i = 0; i < num && unit != null; i++) {
        mCacheIndex1.insert(unit);
				/*
				if(!mCacheIndex1.judgeIfRing())
					throw new RuntimeException();*/
        unit = unit.after;
      }
    }

    private void test1(CacheInternalUnit unit) {
      if (test(unit)) {
        System.out.println(unit);
        mCacheIndex1.print();
        throw new RuntimeException();
      }
    }

    private boolean test(CacheInternalUnit unit) {
      CacheInternalUnit x = (CacheInternalUnit) mCacheIndex1.mRoot;
      long begin = unit.getBegin();
      long end = unit.getEnd();
      while (x != null) {
        if (begin >= x.getBegin() && end <= x.getEnd()) {
          return true;

        } else if (begin >= x.getEnd()) {
          if (x.right != null) {
            x = x.right;
          } else {
            return false;

          }
        } else if (end <= x.getBegin()) {
          if (x.left != null) {
            x = x.left;
          } else {
            return false;

          }
        } else {
          return false;

        }
      }
      return false;
    }

    @Override
    public void deleteInIndex(CacheInternalUnit unit) {
      mCacheIndex1.remove(unit);
      unit.clearTreeIndex();
      test1(unit);
    }

    @Override
    public void addToIndex(CacheInternalUnit unit) {
      mCacheIndex1.insert(unit);
      //if(!mCacheIndex1.judgeIfRing())
      //	throw new RuntimeException();
      //mCacheIndex1.findByIndex()
    }

    @Override
    public CacheUnit findByIndex(long begin, long end) {
      int index = getIndex(begin, end);
      return ClientCacheContext.INSTANCE.getKeyByTree(begin, end, mCacheIndex1, mFileId, index);
    }

    @Override
    public void clearIndex() {
      mCacheIndex1 = null;
    }

  }

  public abstract class LinkBucket {
    /** the begin of the first cache unit in this bucket. */
    // long mBegin;
    /**
     * the begin side of unit in this bucket are small than mEnd.
     */
    // long mEnd;
    CacheInternalUnit mStart;
    CacheInternalUnit mEnd;
    private final int CONVERT_LENGTH = 8;
    int mUnitNum;
    boolean mConvertBefore = false;
    boolean mIsRserveIndex = true;
    private int mIndex;
    // CacheInternalUnit end;

    public LinkBucket(int index) {
      mUnitNum = 0;
      mIndex = index;
    }

    public abstract void convert(CacheInternalUnit unit, int num);

    public abstract void addToIndex(CacheInternalUnit unit);

    public abstract CacheUnit findByIndex(long begin, long end);

    public abstract void deleteInIndex(CacheInternalUnit unit);

    public abstract void clearIndex();

    private void deleteInBucket(CacheInternalUnit unit) {
      if (mUnitNum == 1) {
        mStart = mEnd = null;
        return;
      }
      if (unit.equals(mStart)) {
        if (mUnitNum > 1) {
          mStart = mStart.after;
        } else {
          mStart = null;
        }
      }
      if (unit.equals(mEnd)) {
        if (mUnitNum > 1) {
          mEnd = mEnd.before;
        } else {
          mEnd = null;
        }
      }
    }

    public synchronized void delete(CacheInternalUnit unit) {
      deleteInBucket(unit);
      mUnitNum--;
      if (mConvertBefore) {
        if (mIsRserveIndex) {
          deleteInIndex(unit);
        } else {
          if (mUnitNum < CONVERT_LENGTH) {
            clearIndex();
            mConvertBefore = false;
          } else {
            deleteInIndex(unit);
          }
        }
      }
    }

    public synchronized void addNew(CacheInternalUnit unit) {
      //TODO judge adding lock or not, because add index will happened after
      // lock bucke index, so maybe no need to lock bucket.
      mUnitNum++;
      if (mUnitNum == 1) {
        mStart = mEnd = unit;
      } else if (mStart == null) {
        mStart = unit;
      } else {
        if (unit.getBegin() <= mStart.getBegin()) {
          mStart = unit;
        }
        if (unit.getBegin() >= mEnd.getBegin()) {
          mEnd = unit;
        }
      }

      if (mUnitNum == CONVERT_LENGTH && !mConvertBefore) {
        convert(mStart, mUnitNum);
        mConvertBefore = true;
      } else if (mConvertBefore) {
        addToIndex(unit);
      }

    }

    public CacheUnit find(long begin, long end) {
      if (mUnitNum > CONVERT_LENGTH) {
        return findByIndex(begin, end);
      } else {
        TmpIterator<CacheInternalUnit> iter;
        if (IS_REVERT) {
          iter = new TmpIterator<>(null, mEnd);
          return ClientCacheContext.INSTANCE.getKeyByReverse(begin, end, mFileId, iter, mIndex);
        } else {

          iter = new TmpIterator<>(mStart, null);
          return ClientCacheContext.INSTANCE.getKey(begin, end, mFileId, iter, mIndex);
        }
      }
    }
  }

  public class TmpIterator<T extends LinkNode> implements Iterator<T>, PreviousIterator<T> {
    T current = null;
    T end = null;
    T begin = null;

    TmpIterator(T mBegin, T mEnd) {
      end = mEnd;
      begin = mBegin;
    }

    @Override
    public T next() {
      if (current == null) {
        current = begin;
        return current;
      }
      current = (T) current.after;
      return current;

    }

    @Override
    public T previous() {
      if (current == null) {
        current = end;
        return current;
      }
      current = (T) current.before;
      return current;
    }

    @Override
    public boolean hasNext() {
      if (current == null) {
        return begin != null;
      }
      return current.after != null;
    }

    @Override
    public boolean hasPrevious() {
      if (current == null) {
        return end != null;
      }
      return current.before != null;
    }

    @Override
    public void remove() {
      current = null;
      begin = null;
      end = null;
    }

    @Override
    public T getBegin() {
      return begin;
    }
  }
}
