package alluxio.client.file.cache.test.mt;

import alluxio.client.file.cache.CacheInternalUnit;
import alluxio.client.file.cache.CacheUnit;
import alluxio.client.file.cache.ClientCacheContext;
import alluxio.client.file.cache.TempCacheUnit;
import org.apache.commons.lang3.RandomUtils;

import java.util.*;

public class LRUEvictContext extends BaseEvictContext {
  LinkedList<TmpCacheUnit> mLRUList = new LinkedList<>();
  Set<TmpCacheUnit> accessSet = new HashSet<>();

  public LRUEvictContext(MTLRUEvictor test, ClientCacheContext cacheContext, long userId) {
    super(test, cacheContext, userId);
  }

  public void evict() {

    while (mCacheSize > mCacheCapacity ) {
      LinkedList<TmpCacheUnit> lruList = mLRUList;
      TmpCacheUnit deleteUnit = lruList.pollFirst();
      CacheUnit res = mCacheContext.getCache(deleteUnit.getFileId(), mTestFileLength, deleteUnit.getBegin(), deleteUnit.getEnd(), unlockTask);
      if (res.isFinish()) {
        mCacheSize -= res.getSize();
        mCacheContext.delete((CacheInternalUnit) res);
      }
      accessSet.remove(deleteUnit);
    }
  }

  public List<TmpCacheUnit> getCacheList() {
    return mLRUList;
  }

  public TmpCacheUnit getEvictUnit() {
    return mLRUList.peekFirst();
  }

  public void fakeAccess(TmpCacheUnit unit) {
    if (!accessSet.contains(unit)) {
      mLRUList.addLast(unit);
      accessSet.add(unit);
    } else {
      mLRUList.remove(unit);
      mLRUList.addLast(unit);
    }
  }

  public void fakeRemove(TmpCacheUnit deleteUnit){
    mLRUList.remove(deleteUnit);
    accessSet.remove(deleteUnit);
  }

  public void test () {
    long tmpSum = 0;
    for (TmpCacheUnit u : accessSet) {
      tmpSum += u.getSize();
    }
    if (tmpSum != mCacheSize) {
      throw new RuntimeException();
    }
  }

  @Override
  public void removeByShare(TmpCacheUnit deleteUnit) {
    if (accessSet.contains(deleteUnit)) {
      mLRUList.remove(deleteUnit);
      accessSet.remove(deleteUnit);
    }
  }

  public TmpCacheUnit getMaxPriorityUnit() {
    return mLRUList.peekLast();
  }

  public TmpCacheUnit getSharedEvictUnit() {
    Iterator<TmpCacheUnit> iterator  = mLRUList.iterator();
    double proSum = 0;
    double shareNum = 0;
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
        //System.out.println("sac "+ saveRatio);

        double RandomTmp = RandomUtils.nextDouble(0,1);
        if (RandomTmp > saveRatio) {
          return tmp;
        }
      } else {

        return tmp;
      }

    }
    return getEvictUnit();
  }

  public double getEvictProbability(TmpCacheUnit unit) {
    if (!accessSet.contains(unit)) {
      return 0;
    }
    int size = accessSet.size();
    double interval = (double)size / (double)10;
    for (int i = 0 ;i < mLRUList.size(); i ++) {
      if(mLRUList.get(i).equals(unit)) {

        return 1 - (double)((int)(i / interval)) /(double) 10;
      }
    }
    return 0;
  }


}

