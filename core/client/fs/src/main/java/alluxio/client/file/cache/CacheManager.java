package alluxio.client.file.cache;

import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.io.IOException;

public final class CacheManager {
  protected final ClientCacheContext mCacheContext;
  private CachePolicy evictor;
  public static long mInsertTime = 0;
  private ClientCacheContext.LockManager mLockManager;
  private PromotionPolicy promoter;
  private boolean isPromotion;

  public CacheManager(ClientCacheContext context) {
    mCacheContext = context;
    isPromotion = mCacheContext.isPromotion();
    setPolicy();
    mLockManager = mCacheContext.getLockManager();
  }

  public void setPolicy() {
    if (!isPromotion) {
      evictor = CachePolicy.factory.create(CachePolicy.PolicyName.ISK);
      evictor.init(mCacheContext.getCacheLimit() + mCacheContext.CACHE_SIZE, mCacheContext);
    } else {
      promoter = new PromotionPolicy();
      promoter.init(mCacheContext.getCacheLimit());
    }
  }

  public CachePolicy.PolicyName getCurrentPolicy() {
    return evictor.getPolicyName();
  }


  public int read(TempCacheUnit unit, byte[] b, int off, int readlen, long pos, boolean isAllowCache) throws IOException {
    int res = -1;
    res = unit.lazyRead(b, off, readlen, pos, isAllowCache);
    BaseCacheUnit unit1 = new BaseCacheUnit(pos, pos + res, unit.getFileId());
    unit1.setCurrentHitVal(unit.getNewCacheSize());
    if (!isPromotion) {
      CacheInternalUnit resultUnit = mCacheContext.addCache(unit);
      evictor.fliter(resultUnit, unit1);
      evictor.check(unit);
    } else {
      promoter.filter(unit1);
    }
    unit.getLockTask().unlockAll();
    return res;
  }

  public int read(CacheInternalUnit unit, byte[] b, int off, long pos, int len) {
    try {
      int remaining = unit.positionedRead(b, off, pos, len);
      unit.getLockTask().unlockAllReadLocks();
      BaseCacheUnit currentUnit = new BaseCacheUnit(pos, Math.min(unit.getEnd(), pos + len), unit.getFileId());
      if (!isPromotion) {
        evictor.fliter(unit, currentUnit);
      } else {
        promoter.filter(currentUnit);
      }
      return remaining;
    } catch (Exception e) {
      System.out.println(unit.mTestName);
      throw new RuntimeException(e);
    }
  }

  public int cache(TempCacheUnit unit, long pos, int len, FileCacheUnit unit1) throws IOException {
    return  unit.cache(pos, len, unit1);
  }
}
