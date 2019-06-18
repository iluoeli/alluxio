package alluxio.client.file.cache.Metric;

import alluxio.client.file.cache.core.*;
import alluxio.client.file.cache.struct.LongPair;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import alluxio.client.file.cache.submodularLib.cacheSet.HitCalculator;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class CacheLimitAdaptor extends SlidingWindowAdaptor {

  private CacheSet accessSet;
  private double cacheScaleLimit = 0.5;

  public CacheLimitAdaptor(PromotionPolicy promotionPolicy) {
    super(promotionPolicy);
  }

  public double countVisitSize() {
    long cacheSize = ClientCacheContext.INSTANCE.getAllSize(ClientCacheContext.INSTANCE);
    long hitSum = 0;

    for (long fileId : accessSet.keySet()) {
        Queue<LongPair> tmpQueue = new LinkedBlockingQueue<>();
        Queue<CacheUnit> queue = accessSet.sortCacheMap.get(fileId);
        long visitSum = FileCacheUnit.cacheCoinFiliter(queue, tmpQueue);
        while (!tmpQueue.isEmpty()) {
          LongPair p = tmpQueue.poll();
          CacheUnit unit = ClientCacheContext.INSTANCE.getCache(fileId, ClientCacheContext.INSTANCE.getMetedataCache().getFileLength(fileId),
                  p.getKey(), p.getValue(), new UnlockTask());
          if (unit.isFinish()) {
            hitSum += unit.getSize();
          } else {
            long missSize = ClientCacheContext.INSTANCE.computeIncrese((TempCacheUnit)unit);
            hitSum += (p.getValue() - p.getKey()) - missSize;
          }
        }
      }
    return hitSum / cacheSize;
  }

  public boolean aveCount(CacheSet setInCache) {
    List<BaseCacheUnit> list = new ArrayList<>();
    Iterator<CacheUnit> iter = setInCache.iterator();
    while (iter.hasNext()) {
      list.add((BaseCacheUnit)iter.next());
    }
    list.sort((o1, o2)-> (int) ((o1.getHitValue() - o2.getHitValue()) * 100));
    double lowestHitValue = list.get(0).getHitValue();


    HitCalculator calculator = new HitCalculator(null);
    Iterator<CacheUnit> iterator = accessSet.iterator();
    while (iterator.hasNext()) {
      calculator.countHitRatio((BaseCacheUnit)iterator.next());
    }
    iterator = accessSet.iterator();
    while (iterator.hasNext()) {
      BaseCacheUnit unit = ((BaseCacheUnit)iterator.next());
      if (unit.getHitValue() >= lowestHitValue) {
         //todo
      }
    }
     return false;
  }

  private void changeRatio(CacheSet setInCache) {
    if (isHitRatioDown()) {
      if (countVisitSize() < cacheScaleLimit) {
        mParameterRatio =Math.min(mLowerBound, mParameterRatio - mInterval);
      } else {
        if (aveCount(setInCache)) {
          mParameterRatio =Math.max(mUpperBound, mParameterRatio + mInterval);
        }
      }
    } else if (isHitRatioUp()) {

    }
    for (int i = 0 ;i < moveNum; i++) {
      mHitRatioCollector.removeLast();
    }
  }
}
