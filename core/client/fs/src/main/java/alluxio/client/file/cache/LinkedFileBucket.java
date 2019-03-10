package alluxio.client.file.cache;

import alluxio.client.file.cache.struct.LinkNode;
import alluxio.client.file.cache.struct.RBTree;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.util.Iterator;

import static alluxio.client.file.cache.ClientCacheContext.fileId;

public class LinkedFileBucket {
  public LinkBucket[] mCacheIndex0;
  /**
   * The num of fileBucket of one file
   */
  private int BUCKET_LENGTH;
  /**
   * The length of one bucket.
   */
  public long mBucketLength;
  private final boolean IS_REVERT;
  private final long mFileId;
  private ClientCacheContext mCacheContext;
  private final ClientCacheContext.LockManager mLockManager;

  public LinkedFileBucket(long fileLength, long fileId, ClientCacheContext context) {
    mCacheContext = context;
    BUCKET_LENGTH = mCacheContext.BUCKET_LENGTH;
    IS_REVERT = mCacheContext.REVERSE;
    mFileId = fileId;
    mLockManager = context.getLockManager();
    mBucketLength = (long) (fileLength / (double) BUCKET_LENGTH);
    if (fileLength % mBucketLength != 0) {
      BUCKET_LENGTH++;
    }
    mCacheIndex0 = new LinkBucket[BUCKET_LENGTH];
    initBucketLock();
  }

  private void initBucketLock() {
    for (int  i = 0 ; i < BUCKET_LENGTH; i ++) {
      mLockManager.initBucketLock(mFileId, i);
    }
  }

  public void clearSaveIndex() {
    for (LinkBucket bucket : mCacheIndex0) {
      bucket.clearIndex();
    }
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

  private void readLockWithInit(int index) {
      mLockManager.readLock(fileId, index, "find");

  }

  public CacheUnit find(long begin, long end, LockTask newTask) {
    int index = getIndex(begin, end);
    newTask.readLock(index);
    CacheUnit unit =  find0(index, begin, end, newTask );
    unit.setLockTask(newTask);
    return unit;
  }


  public CacheUnit find0(int index, long begin, long end, LockTask task) {
    LinkBucket bucket = mCacheIndex0[index];
    //boolean needUnlockIndex = true;
    if (bucket == null || bucket.mUnitNum == 0) {
      int left, right;
      left = right = index;
      while (true) {
        if (left == right) task.readUnlock(index);
        left--;
        right++;
        if (left < 0) {
          task.readLock(0);
          return mCacheContext.getKeyFromBegin(begin, end, mFileId, task);
        } else if (right >= BUCKET_LENGTH) {
          task.readLock(BUCKET_LENGTH - 1);
          return mCacheContext.getKeyByReverse2(begin, end, mFileId, BUCKET_LENGTH - 1, task);
        } else {
          task.readLock(right);
          if (mCacheIndex0[right] != null && mCacheIndex0[right].mUnitNum > 0) {
            CacheInternalUnit before = mCacheIndex0[right].mStart.before;
            Iterator<CacheInternalUnit> iter = new TmpIterator<>(mCacheIndex0[right].mStart, null);
            CacheUnit unit = mCacheContext.getKey(begin, end, mFileId, iter, right, task);
            if (unit.isFinish()) return unit;
            TempCacheUnit tmp = (TempCacheUnit) unit;
            if (before != null && tmp.getBegin() < before.getEnd()) {
              return mCacheContext.handleRightCoincidence(tmp, before, true, index, task);
            } else {
              return tmp;
            }
          }
          task.readUnlock(right);
          task.readLock(left);
          if (mCacheIndex0[left] != null && mCacheIndex0[left].mUnitNum > 0) {
            task.readUnlock(right);
            CacheInternalUnit after = mCacheIndex0[left].mEnd.after;
            PreviousIterator<CacheInternalUnit> iter = new TmpIterator<>(null, mCacheIndex0[left].mEnd);
            CacheUnit unit = mCacheContext.getKeyByReverse(begin, end, mFileId, iter, left, task);
            if (unit.isFinish()) return unit;
            TempCacheUnit tmp = (TempCacheUnit) unit;
            if (after != null && tmp.getEnd() > after.getBegin()) {
              return mCacheContext.handleLeftCoincidence(after, tmp, true, index, task);
            } else {
              return tmp;
            }
          }
            task.readUnlock(left);
          }
        }

    }
    return bucket.find(begin, end, task);
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
      CacheInternalUnit uu = test(unit);
      if (uu != null) {
        System.out.println(unit);
        mCacheIndex1.print();
        throw new RuntimeException();
      }
    }

    private CacheInternalUnit test(CacheInternalUnit unit) {
      CacheInternalUnit x = (CacheInternalUnit) mCacheIndex1.mRoot;
      long begin = unit.getBegin();
      long end = unit.getEnd();
      while (x != null) {
        if (begin >= x.getBegin() && end <= x.getEnd()) {
          return x;
        } else if (begin >= x.getEnd()) {
          if (x.right != null) {
            x = x.right;
          } else {
            return null;
          }
        } else if (end <= x.getBegin()) {
          if (x.left != null) {
            x = x.left;
          } else {
            return null;
          }
        } else {
          return null;
        }
      }
      return null;
    }

    @Override
    public void deleteInIndex(CacheInternalUnit unit) {
      mCacheIndex1.remove(unit);
      unit.clearTreeIndex();
      // TODO make the test function running as async thread
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
    public CacheUnit findByIndex(long begin, long end, LockTask task) {
      int index = getIndex(begin, end);
      return mCacheContext.getKeyByTree(begin, end, mCacheIndex1, mFileId, index, task);
    }

    @Override
    public void clearIndex() {
      mCacheIndex1 = null;
    }
  }

  abstract class LinkBucket {
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

    public abstract CacheUnit findByIndex(long begin, long end, LockTask task);

    public abstract void deleteInIndex(CacheInternalUnit unit);

    public abstract void clearIndex();

    private void deleteInBucket(CacheInternalUnit unit) {
      if (mUnitNum == 0) {
        mStart = mEnd = null;
        return;
      }
      if (unit.equals(mStart)) {
        if (mUnitNum > 0) {
          mStart = mStart.after;
        } else {
          mStart = null;
        }
      }
      if (unit.equals(mEnd)) {
        if (mUnitNum > 0) {
          mEnd = mEnd.before;
        } else {
          mEnd = null;
        }
      }
    }

    public void testBefore() {
      if (mConvertBefore) {
        ((RBTreeBucket) this).mCacheIndex1.print();
      }
    }

    public void test() {
      if (mConvertBefore) {
        CacheInternalUnit unit = mStart;
        while (unit != mEnd.after) {
          if (unit == null) {
            test1();
            ((RBTreeBucket) this).mCacheIndex1.print();
            throw new RuntimeException();
          }
          CacheUnit u = find(unit.getBegin(), unit.getEnd(), new UnlockTask());
          if (!u.isFinish()) {
            unit = mStart;
            while (unit != mEnd.after) {
              System.out.print(unit.getBegin() + " " + unit.getEnd() + " || ");
              unit = unit.after;
            }
            System.out.println();
            ((RBTreeBucket) this).mCacheIndex1.print();
            throw new RuntimeException();
          }
          unit = (CacheInternalUnit) unit.after;
        }
      }
    }

    public void test1() {
      System.out.println("start :::: " + mStart);
      System.out.println("end :::: " + mEnd);
      if (mStart != null) System.out.println("start after :: " + mStart.after);
      if (mEnd != null) System.out.println("end before :: " + mEnd.before);

      System.out.println("num : " + mUnitNum);
    }

    public void delete(CacheInternalUnit unit) {
      mUnitNum--;
      deleteInBucket(unit);
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

    public void addNew(CacheInternalUnit unit) {
      //TODO judge adding lock or not, because add index will happened after
      // lock bucke index, so maybe no need to lock bucket.
      mUnitNum++;
      if (mUnitNum == 1) {
        mStart = mEnd = unit;
      } else {
        if (mStart == null) {
          mStart = unit;
        } else {
          if (IS_REVERT && unit.getEnd() < mStart.getEnd()) {
            mStart = unit;
          }
          if (!IS_REVERT && unit.getBegin() < mStart.getBegin()) {
            mStart = unit;
          }
        }
        if (mEnd == null) {
          mEnd = unit;
        } else {
          if (IS_REVERT && unit.getEnd() > mEnd.getEnd()) {
            mEnd = unit;
          }
          if (!IS_REVERT && unit.getBegin() > mEnd.getBegin()) {
            mEnd = unit;
          }
        }
      }

      if (mUnitNum == CONVERT_LENGTH && !mConvertBefore) {
        convert(mStart, mUnitNum);
        mConvertBefore = true;
      } else if (mConvertBefore) {
        addToIndex(unit);
      }
    }

    public CacheUnit find(long begin, long end, LockTask task) {
      if (mUnitNum > CONVERT_LENGTH || mConvertBefore) {
        return findByIndex(begin, end, task);
      } else {
        TmpIterator<CacheInternalUnit> iter;
        int index = mIndex;
        if (IS_REVERT) {
          if (end > mEnd.getEnd() && mEnd.after != null) {
            iter = new TmpIterator<>(null, mEnd.after);
            index++;
            task.readLock(index);
          } else {
            iter = new TmpIterator<>(null, mEnd);
          }
          return mCacheContext.getKeyByReverse(begin, end, mFileId, iter, index, task);
        } else {
          if (begin < mStart.getBegin() && mStart.before != null && index != 0) {
            iter = new TmpIterator<>(mStart.before, null);
            index--;
            task.readLock(index);
          } else {
            iter = new TmpIterator<>(mStart, null);
          }
          return mCacheContext.getKey(begin, end, mFileId, iter, index, task);
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
