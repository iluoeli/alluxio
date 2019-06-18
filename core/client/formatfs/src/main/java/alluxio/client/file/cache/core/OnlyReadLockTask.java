package alluxio.client.file.cache.core;

public class OnlyReadLockTask extends LockTask {

  public OnlyReadLockTask(ClientCacheContext.LockManager lockManager) {
    super(lockManager);
  }

  @Override
  public void writeLock(int index) {
    readLock(index);
  }
  @Override
  public void unlockAllWriteLocks() {
    unlockAllReadLocks();
  }
}
