package alluxio.client.file.cache.submodularLib;

import alluxio.client.file.cache.core.*;
import alluxio.client.file.cache.struct.LongPair;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import org.apache.commons.lang3.RandomUtils;

import java.util.*;

public class LRUEvictor {
  LinkedList<BaseCacheUnit> accessList = new LinkedList<>();
  public ClientCacheContext mContext;
  private Map<CacheInternalUnit, Set<BaseCacheUnit>> mAccessMap = new HashMap<>();
  public long mTestFileId = 1;
  //cheat test set to 100 the cache evictor need to set to lfu
  public long mTestFileLength = 1024 * 1024 * 1024;
  protected Set<BaseCacheUnit> visitList = new HashSet<>();
  public UnlockTask unlockTask = new UnlockTask();
  //cheat test set to 100
  public long cacheSize = 1024 * 1024 * 300;

  public LRUEvictor(ClientCacheContext context) {
    mContext = context;
  }

  private void add(BaseCacheUnit unit) {
    CacheUnit unit1 = mContext.getCache(mTestFileId, mTestFileLength, unit.getBegin(), unit.getEnd(),
      new UnlockTask());
    if (!unit1.isFinish()) {
      TempCacheUnit u = (TempCacheUnit)unit1;
      List<CacheInternalUnit> tmp = new ArrayList<>(u.mCacheConsumer);
      CacheInternalUnit newUnit = mContext.addCache((TempCacheUnit) unit1);
      Set<BaseCacheUnit> newSet = new HashSet<>();
      for (CacheInternalUnit tmpUnit : tmp) {
        newSet.addAll(mAccessMap.get(tmpUnit));
      }
      for (CacheInternalUnit tmpUnit : tmp) {
        mAccessMap.remove(tmpUnit);
      }
      mAccessMap.put(newUnit, newSet);
      for (BaseCacheUnit tmp1 : newSet) {
        if (tmp1.isCoincience(unit)) {
          accessList.remove(tmp1);
          accessList.addLast(tmp1);
        }
      }
      newSet.add(unit);
    } else {
      CacheInternalUnit u = (CacheInternalUnit)unit1;
      for (BaseCacheUnit tmp1 : mAccessMap.get(u)) {
        if (tmp1.isCoincience(unit)) {
          accessList.remove(tmp1);
          accessList.addLast(tmp1);
        }
      }
      mAccessMap.get(u).add(unit);
    }
  }

  private Queue<LongPair> getReserveSpace(CacheInternalUnit unit, BaseCacheUnit unit1) {
    long deleteBegin = unit1.getBegin();
    long deleteEnd = unit1.getEnd();
    Queue<LongPair> res = new LinkedList<>();
    if (unit.getBegin() < deleteBegin) {
      res.add(new LongPair(unit.getBegin(), unit1.getBegin()));
    }

    if (unit.getEnd() > deleteEnd) {
      res.add(new LongPair(deleteEnd, unit.getEnd()));
    }
    return res;
  }

  private long delete0(CacheInternalUnit unit2, BaseCacheUnit unit) {
    long deleteSize;
    Queue<LongPair> tmp = getReserveSpace(unit2, unit);
    if(!tmp.isEmpty()) {
      long s = unit2.getSize();
      unit2.splitTest(tmp,  mContext.mFileIdToInternalList.get(mTestFileId).mBuckets);
      deleteSize = unit2.getDeleteSize();
      mContext.delete(unit2);
    } else {
      deleteSize = mContext.delete(unit2);
    }
    return deleteSize;
  }

  private long delete(BaseCacheUnit unit) {
    CacheUnit unit1 = mContext.getCache(mTestFileId, mTestFileLength, unit.getBegin(), unit.getEnd(), new UnlockTask());
    long deleteSize;
    if (unit1.isFinish()) {
      CacheInternalUnit unit2 = (CacheInternalUnit)unit1;
      return delete0(unit2, unit);
    } else {
      long delete = 0;
      TempCacheUnit unit2 = (TempCacheUnit) unit1;
      for (CacheInternalUnit u : unit2.mCacheConsumer) {
        delete += delete0(u, unit);
      }
      deleteSize = delete;
    }
    return deleteSize;
  }


  public void init(long limit) throws Exception {
    long sum = 0;
    CacheSet s = new CacheSet();
    for(int i = 0 ; i < 1200; i ++) {
      long length = 1024 * 1024;
      long begin = RandomUtils.nextLong(0, 1024 * 1024 * 1024 - length);
      sum += length;
      BaseCacheUnit unit = new BaseCacheUnit(mTestFileId, begin, begin + length);
      add(unit);
      s.addSort(unit);
      s.add(unit);
      visitList.add(unit);

    }
    long size = mContext.getAllSize(s, mContext);
    evict(size, limit);
    System.out.println("size : " + mContext.getAllSize(mContext)/(1024 * 1024));
  }

  public void evict(long size, long limit) {
    while (size > limit) {
      BaseCacheUnit unit = accessList.getFirst();
      accessList.removeFirst();
      size -= delete(unit);
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

  public static void main(String[] args) throws Exception{
    LRUEvictor test = new LRUEvictor(new ClientCacheContext(false));
    test.init(1024 * 1024 * 600);
    test.testVisit();
  }
}
