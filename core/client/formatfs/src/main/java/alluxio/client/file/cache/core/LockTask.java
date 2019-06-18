package alluxio.client.file.cache.core;

import com.google.common.base.Preconditions;

import java.util.HashSet;
import java.util.Set;

public class LockTask {
  private Set<Integer> mWriteLockIndex = new HashSet<>();
  private Set<Integer> mReadLockIndex = new HashSet<>();
  private ClientCacheContext.LockManager mLockManger;
  private long mFileId = -1;

  public LockTask(ClientCacheContext.LockManager lockManager, long fileId) {
    mFileId = fileId;
    mLockManger = lockManager;
  }

  public LockTask(ClientCacheContext.LockManager lockManager) {
    mLockManger = lockManager;
  }

  public void setFileId(long fileId) {
    mFileId = fileId;
  }


  public void readLock(int index) {
    if (!mReadLockIndex.contains(index)) {
      mReadLockIndex.add(index);
      mLockManger.readLock(mFileId, index, "");
    }
  }

  public void readUnlock(int index) {
    if (mReadLockIndex.contains(index)) {
      mLockManger.readUnlockWithRetry(mFileId, index);
      mReadLockIndex.remove(new Integer(index));
    }
  }

  public void writeLock(int index) {
    if (!mWriteLockIndex.contains(index)) {
      mLockManger.writeLock(mFileId, index);
      mWriteLockIndex.add(index);
    }
  }

  public void unlockAllWriteLocks() {
    if (!mWriteLockIndex.isEmpty()) {
      for (int i : mWriteLockIndex) {
        mLockManger.writeUnlock(mFileId, i);
      }
      mWriteLockIndex.clear();
    }
  }

  public void deleteLock(CacheInternalUnit unit) {
    writeLock(unit.mBucketIndex);
    if (unit.before != null && unit.before.mBucketIndex != unit.mBucketIndex) {
      mLockManger.writeLock(unit.before.mBucketIndex, Math.max(unit.mBucketIndex - 1, 0 ), this);
    }
    if (unit.after != null && unit.after.mBucketIndex != unit.mBucketIndex) {
      mLockManger.writeLock(unit.mBucketIndex + 1, unit.after.mBucketIndex, this);
    }
  }

  public void deleteUnlock() {
    Preconditions.checkArgument(mReadLockIndex.isEmpty());
    unlockAllWriteLocks();
  }

  public void unlockAllReadLocks() {
    if (!mReadLockIndex.isEmpty()) {
      for (int i : mReadLockIndex) {
        mLockManger.readUnlockWithRetry(mFileId, i);
      }
      mReadLockIndex.clear();
    }
  }

  public void unlockAll() {
    unlockAllReadLocks();
    unlockAllWriteLocks();
  }

  public void lockUpgrade() {
    for (int readIndex : mReadLockIndex) {
      mLockManger.readUnlockWithRetry(mFileId, readIndex);
      writeLock(readIndex);
    }
    mReadLockIndex.clear();
  }

  @Override
  public String toString() {
    String res = "write lock : ";
    for (int i : mWriteLockIndex) {
      res = res + " " +  i ;
    }
    res += "read lock : ";
    for (int i : mReadLockIndex) {
      res = res + " " + i;
    }
    return res;
  }
}
