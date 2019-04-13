package alluxio.client.file.cache.test.mt;

import alluxio.client.file.cache.ClientCacheContext;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomUtils;

import java.util.*;

public class LFUEvictContext extends BaseEvictContext {
  public PriorityQueue<TmpCacheUnit> mVisitQueue;
  Map<TmpCacheUnit, Integer> mAccessMap = new HashMap<>();
  private int time = 0;
  private ArrayList<TmpCacheUnit> tmpSortList;

  public LFUEvictContext(MTLRUEvictor test, ClientCacheContext cacheContext, long userId) {
    super(test, cacheContext, userId);
    mVisitQueue = new PriorityQueue<>();
  }

  public void fakeAccess(TmpCacheUnit unit) {
    unit.setmAccessInterval(++ time);
   // check();
    if (mAccessMap.containsKey(unit)) {
      mVisitQueue.remove(unit);
      mVisitQueue.add(unit.setmAccessTime(mAccessMap.get(unit) +1));
    }
    else {
      mVisitQueue.add(unit.getmAccessInterval()!= 0? unit : unit.setmAccessTime(1));
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

  public void sort() {
    tmpSortList = new ArrayList<>(mVisitQueue);
    Collections.sort(tmpSortList);
  }

  public TmpCacheUnit getSharedEvictUnit() {
    double proSum = 0;
    double shareNum = 0;

    sort();
    Preconditions.checkArgument(tmpSortList != null);
    Iterator<TmpCacheUnit> iterator = tmpSortList.iterator();
    while (iterator.hasNext()) {
      TmpCacheUnit tmp = iterator.next();
      Set<Long> s = mtlruEvictor.mShareSet.get(tmp);
      if (s.size() > 1) {

        for (long l : s) {
          if (l != mUserId) {
            double pro = mtlruEvictor.actualEvictContext.get(l).getEvictProbability(tmp);
            proSum += pro;
            shareNum ++;
          }
        }
        double saveRatio = 1 - proSum / shareNum;
        //System.out.println(saveRatio);
        double RandomTmp = RandomUtils.nextDouble(0,1);
        if (RandomTmp > saveRatio) {
         // System.out.println(saveRatio + " " + RandomTmp);

          return tmp;
        }
      } else {
      return tmp;
    }

    }
    return getEvictUnit();
  }

  public double getEvictProbability(TmpCacheUnit unit) {
    if (!mAccessMap.containsKey(unit)) {
      return 0;
    }
    int size = mAccessMap.size();
    double interval = (double)size / (double)10;
    if (tmpSortList == null) {
      sort();
    }
    for (int i = 0 ;i <tmpSortList.size(); i ++) {
      if(tmpSortList.get(i).equals(unit)) {
        return 1 - (double)((int)(i / interval)) /(double) 10;
      }
    }
    return 0;
  }


  public void print() {
    sort();
    System.out.println("============begin===========");
    System.out.println("print info " + mUserId);
    Map<Long, Integer> tmp = new HashMap<>();

    for (TmpCacheUnit unit :tmpSortList) {
      tmp.put(unit.getFileId(), tmp.getOrDefault(unit.getFileId(), 0) + 1);
    }
    for (long l : tmp.keySet()) {
      System.out.println("user id " + l + " " + tmp.get(l));
    }
    System.out.println("============finish===========");
  }
}
