package alluxio.client.file.cache.mt.test;

import alluxio.client.HitMetric;
import alluxio.client.file.cache.core.*;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import alluxio.client.file.cache.submodularLib.cacheSet.DivideGR;
import org.apache.commons.lang3.RandomUtils;

public class DivideGrEvictor extends LRUEvictor {
  private DivideGR divideGR = new DivideGR();
  private long mCacheCapacity = 1024 * 1024 * 1024;

  public DivideGrEvictor(ClientCacheContext mContext) {
    super(mContext);
  }

  public void init() throws Exception {
    divideGR.init(mCacheCapacity, mContext);
    long sum = 0;
    CacheSet s = new CacheSet();
    for(int i = 0 ; i < 1024 * 2; i ++) {
      long length = RandomUtils.nextLong(1024 * 1024, 1024 * 1024 * 4);
      long begin = RandomUtils.nextLong(0, 1024 * 1024 * 1024 - length);
      sum += length;
      BaseCacheUnit unit = new BaseCacheUnit(mTestFileId, begin, begin + length);
      add(unit);
      s.addSort(unit);
      s.add(unit);
      visitList.add(unit);
    }
    long size = mContext.getAllSize(s, mContext);
//    System.out.println("size : " + mContext.getAllSize(mContext)/(1024 * 1024));
//    divideGR.test();
    System.out.println(divideGR.evict(size, 1024 * 1024 * 100));
    System.out.println("size : " + mContext.getAllSize(mContext)/(1024 * 1024));
  }

  private long add(BaseCacheUnit unit) {
    CacheUnit unit1 = mContext.getCache(unit.getFileId(), mTestFileLength, unit.getBegin(), unit.getEnd(),
      new UnlockTask());

    long hitSize = 0;
    CacheInternalUnit newUnit;
    if (!unit1.isFinish()) {
      long newSize = mContext.computeIncrese((TempCacheUnit)unit1);
      TempCacheUnit tmpUnit = (TempCacheUnit)unit1;
      byte[] b = new byte[(int)tmpUnit.getSize()];
      hitSize += (unit.getSize() - newSize);
      newUnit = mContext.addCache((TempCacheUnit) unit1);
    } else {
      newUnit = (CacheInternalUnit)unit1;
      hitSize += unit.getSize();
    }
    HitMetric.mHitSize += hitSize;
    HitMetric.mMissSize += (unit.getSize() - hitSize);
    divideGR.fliter(newUnit, unit);
    return unit.getSize() - hitSize;
  }

  public void readWithEvict() {
    divideGR.init(mCacheCapacity, mContext);
    for(int i = 0 ; i < 1024 * 2; i ++) {
      System.out.println("test " + i);
      long length = RandomUtils.nextLong(1024 * 1024, 1024 * 1024 * 4);
      int fileId = RandomUtils.nextInt(0, 2);
      long begin = RandomUtils.nextLong(0, 1024 * 1024 * 1024 - length);
      BaseCacheUnit unit = new BaseCacheUnit(mTestFileId +fileId, begin, begin + length);
      long newSize = add(unit);
      System.out.println("new : "+ (double)newSize /(double) (1024 * 1024));
      divideGR.check(newSize);
    }
  }

  public void testVisit() {
    double visitTime = 0;
    long visitSize = 0;
    long allVisitSize = 0;
    for (BaseCacheUnit unit : visitList) {
      allVisitSize += unit.getSize();
      CacheUnit unit1 = mContext.getCache(mTestFileId, mTestFileLength, unit.getBegin(), unit.getEnd(), new UnlockTask());
      if (unit1.isFinish()) {
        visitSize += unit.getSize();
        visitTime ++;
      } else {
        TempCacheUnit unit2 = (TempCacheUnit)unit1;
        long m = 0;
        for (CacheInternalUnit unit3 : unit2.mCacheConsumer) {
          m += unit3.getSize();
        }
        long missSize = unit2.getSize() - m;
        long hitSize = unit.getSize() - missSize;
        visitSize += hitSize;
        visitTime += ((double)hitSize /(double) unit.getSize());
      }
    }
    System.out.println("hitRatio by size : " +( (double)visitSize / (double)allVisitSize ));
    System.out.println("hitRatio by time : " + (visitTime / (double)visitList.size()));
  }

  public static void main(String[] args) throws Exception {
    DivideGrEvictor t = new DivideGrEvictor(new ClientCacheContext(false));
    t.readWithEvict();
    System.out.println("hit ratio " + (double) HitMetric.mHitSize / (double)(HitMetric.mMissSize +
      HitMetric.mHitSize));
  }

}
