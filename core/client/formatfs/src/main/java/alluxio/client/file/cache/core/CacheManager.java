package alluxio.client.file.cache.core;

import alluxio.client.HitMetric;
import alluxio.client.file.CacheParamSetter;
import alluxio.client.file.cache.Metric.ClientCacheStatistics;
import alluxio.client.file.cache.Metric.HitRatioMetric;

import java.io.IOException;

public final class CacheManager {
  protected final ClientCacheContext mCacheContext;
  private CachePolicy evictor;
  private PromotionPolicy promoter;
  private boolean isPromotion;

  public boolean isFixedLength() {
    return evictor.isFixedLength();
  }

  public CacheManager(ClientCacheContext context) {
    mCacheContext = context;
    isPromotion = mCacheContext.isPromotion();
    setPolicy();
  }

  public void setPolicy() {
    if (!isPromotion) {
//      evictor = CachePolicy.factory.create(CachePolicy.PolicyName.ISK);
        evictor = CachePolicy.factory.create(CacheParamSetter.POLICY_NAME);
      evictor.init(mCacheContext.getCacheLimit() + mCacheContext.CACHE_SIZE, mCacheContext);
    } else {
      promoter = new PromotionPolicy();
      promoter.init(mCacheContext.getCacheLimit());
    }
  }

  public int read(TempCacheUnit unit, byte[] b, int off, int readlen, long pos, boolean isAllowCache) throws IOException {
    int res = -1;
    long begin = System.currentTimeMillis();
    long st = System.currentTimeMillis();
    res = unit.lazyRead(b, off, readlen, pos, isAllowCache);
    ClientCacheStatistics.INSTANCE.lazyReadTime += (System.currentTimeMillis() - st);
    BaseCacheUnit unit1 = new BaseCacheUnit(unit.getFileId(), pos, pos + res);
    unit1.setCurrentHitVal(unit.getNewCacheSize());
    HitMetric.mMissSize += unit.getNewCacheSize();
    if (!isPromotion) {
      if (isAllowCache) {
        long evictST = System.currentTimeMillis();
        CacheInternalUnit resultUnit = mCacheContext.addCache(unit);
        FileCacheUnit uu = mCacheContext.mFileIdToInternalList.get(unit.getFileId());
        int i = uu.mBuckets.getIndex(resultUnit.getBegin(), resultUnit.getEnd());
        long testSt = System.currentTimeMillis();
//        uu.mBuckets.mCacheIndex0[i].test();
        ClientCacheStatistics.INSTANCE.testTime += (System.currentTimeMillis() - testSt);
        evictor.fliter(resultUnit, unit1);
        evictor.check(unit);
        ClientCacheStatistics.INSTANCE.evictTime += (System.currentTimeMillis() - evictST);
      } else {
        evictor.fliter(null, unit1);
      }
    } else {
      promoter.filter(unit1);
    }
    unit.getLockTask().unlockAll();
    testRead.tmpRead += System.currentTimeMillis() - begin;
    HitRatioMetric.INSTANCE.accessSize += readlen;
    return res;
  }

  public int read(CacheInternalUnit unit, byte[] b, int off, long pos, int len) {
    try {
      long begin = System.currentTimeMillis();

      int remaining = unit.positionedRead(b, off, pos, len);
      HitMetric.mHitSize += len;
      unit.getLockTask().unlockAllReadLocks();
      BaseCacheUnit currentUnit = new BaseCacheUnit(unit.getFileId(), pos, Math.min(unit.getEnd
        (), pos + len));
      if (!isPromotion) {
        evictor.fliter(unit, currentUnit);
      } else {
        promoter.filter(currentUnit);
      }
      testRead.cacheRead += System.currentTimeMillis() - begin;
      HitRatioMetric.INSTANCE.accessSize += len;

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
