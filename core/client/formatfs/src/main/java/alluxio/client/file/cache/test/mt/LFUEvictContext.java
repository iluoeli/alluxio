package alluxio.client.file.cache.test.mt;

import alluxio.client.file.cache.ClientCacheContext;

import java.util.*;

public class LFUEvictContext extends BaseEvictContext {
  private PriorityQueue<TmpCacheUnit> mVisitQueue;
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


  public List<TmpCacheUnit> getCacheList() {
    return new ArrayList<>(mVisitQueue);
  }

  public long access(TmpCacheUnit unit) {
    if (mAccessMap.containsKey(unit)) {
      mVisitQueue.remove(unit);
      mVisitQueue.add(unit.setmAccessTime(mAccessMap.get(unit) +1));
    }
    else {
      mVisitQueue.add(unit.setmAccessTime(1));
    }
    mAccessMap.put(unit, mAccessMap.getOrDefault(mAccessMap.get(unit), 1));
    return access0(unit);
  }

  public long remove(TmpCacheUnit unit){
    mAccessMap.remove(unit);
    mVisitQueue.remove(unit);
    return remove0(unit);
  }

  public TmpCacheUnit getEvictUnit() {
    return mVisitQueue.peek();
  }

  public TmpCacheUnit getMaxPriorityUnit() {
    return null;
  }

  public void evict() {
    while (mCacheSize > mCacheCapacity ) {
      TmpCacheUnit deleteUnit = mVisitQueue.peek();
      mCacheSize -= remove(deleteUnit);
    }
  }

  @Override
  public void removeByShare(TmpCacheUnit deleteUnit) {
    if (mAccessMap.containsKey(deleteUnit)) {
      mAccessMap.remove(deleteUnit);
      mVisitQueue.remove(deleteUnit);
      if (mStoreSet.contains(deleteUnit)) {
        mStoreSet.remove(deleteUnit);
        mCacheSize -= deleteUnit.getSize();
      }
    }
  }
}
