package alluxio.client.file.cache.test.mt;

import alluxio.client.file.cache.ClientCacheContext;
import com.google.common.base.Preconditions;

import java.util.*;

public class LFUEvictContext extends BaseEvictContext {
  public PriorityQueue<TmpCacheUnit> mVisitQueue;
  Map<TmpCacheUnit, Integer> mAccessMap = new HashMap<>();

  public LFUEvictContext(MTLRUEvictor test, ClientCacheContext cacheContext, long userId) {
    super(test, cacheContext, userId);
    mVisitQueue = new PriorityQueue<>(new Comparator<TmpCacheUnit>() {
      @Override
      public int compare(TmpCacheUnit o1, TmpCacheUnit o2) {
        return o1.getmAccessTime() - o2.getmAccessTime();
      }
    });
  }

  public void fakeAccess(TmpCacheUnit unit) {
   // check();
    if (mAccessMap.containsKey(unit)) {
      mVisitQueue.remove(unit);
      mVisitQueue.add(unit.setmAccessTime(mAccessMap.get(unit) +1));
    }
    else {
      mVisitQueue.add(unit.setmAccessTime(1));
    }
    mAccessMap.put(unit, mAccessMap.getOrDefault(mAccessMap.get(unit), 1));
  }


  public List<TmpCacheUnit> getCacheList() {
    return new ArrayList<>(mVisitQueue);
  }

  public void fakeRemove(TmpCacheUnit unit){
    mAccessMap.remove(unit);
    mVisitQueue.remove(unit);
  }

  /*
  void check() {
    double size = 0;
    for (TmpCacheUnit unit1 : mVisitQueue) {
      if (mtlruEvictor.mShareSet.containsKey(unit1)) {
        size += ((double)unit1.getSize() / (double) mtlruEvictor.mShareSet.get(unit1).size());
      }
    }
    int tmp = (int)(size / (double)( 1000 * 1000));
    int tmp2 = (int)(mCacheSize/(double)(1000 * 1000));
    System.out.println(tmp + " "  +tmp2);
    if (Math.abs(tmp - tmp2) >= 1) {
      throw new RuntimeException("wrong!" + size + " " + mCacheSize);
    }
  }*/

  public TmpCacheUnit getEvictUnit() {
    return mVisitQueue.peek();
  }

  public TmpCacheUnit getMaxPriorityUnit() {
    return null;
  }

  public void evict() {
    while (mCacheSize > mCacheCapacity ) {
      TmpCacheUnit deleteUnit = mVisitQueue.peek();
      mCacheSize -= remove(deleteUnit, false);
    }
   }


  @Override
  public void removeByShare(TmpCacheUnit deleteUnit) {
    if (mAccessMap.containsKey(deleteUnit)) {
      mAccessMap.remove(deleteUnit);
      mVisitQueue.remove(deleteUnit);
    }
  }
}
