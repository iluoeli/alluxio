package alluxio.client.file.cache;

import alluxio.client.file.cache.submodularLib.cacheSet.CacheHitCalculator;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSetUtils;

import java.util.Iterator;

public enum GhostCache {
  INSTANCE;
  private CacheHitCalculator mGhostCacheCalculator = new CacheHitCalculator(new CacheSetUtils());
  public double mGhostHit = 0.3;

  public synchronized void add(BaseCacheUnit unit) {
    mGhostCacheCalculator.add(unit);
  }

  public void clear() {
    mGhostCacheCalculator.mHitRatioSortMap.clear();
  }

  public double statisticsHitRatio(CacheSet input) {
    Iterator<CacheUnit> iter = input.iterator();
    double res = 0;
    while (iter.hasNext()) {

      CacheUnit unit = iter.next();
      res += mGhostCacheCalculator.staticHitRatioFoGhost(unit);
    }
    return res;
  }

  public static void main(String[] args) {
    GhostCache cache = GhostCache.INSTANCE;
    for (int i = 0; i < 10; i++) {
      BaseCacheUnit unit = new BaseCacheUnit(i * 10, i * 10 + 10, 1);
      cache.add(unit);
    }
    BaseCacheUnit unit1 = new BaseCacheUnit(0, 5, 1);
    //BaseCacheUnit unit2 = new BaseCacheUnit(40 , 50,1);
    //BaseCacheUnit unit3 = new BaseCacheUnit(90 , 100,1);
    CacheSet input = new CacheSet();
    //input.add(unit2);
    input.add(unit1);
    //input.add(unit3);
    System.out.println(cache.statisticsHitRatio(input));
  }
}
