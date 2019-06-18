package alluxio.client.file.cache.submodularLib;

import alluxio.client.file.cache.core.*;
import alluxio.client.file.cache.mt.run.TmpCacheUnit;
import org.apache.commons.lang3.RandomUtils;

import java.util.*;

public class TraditionalLFUEvictor implements CachePolicy {
  public PriorityQueue<TmpCacheUnit> mVisitQueue;
  private Map<Long, Map<Integer, Integer>> visitMap = new HashMap<>();
  public long blockSize = (long) (1024 * 1024 * 2 );
  private double mVisitSize = 0;
  private double mHitSize = 0;
  public long limit = 1024 * 1024 * 400;
  public long mPromoteSize = 0;
  protected Set<BaseCacheUnit> visitList = new HashSet<>();
  public long mTestFileId = 1;
  protected ClientCacheContext mContext;


  public boolean isSync() {
    return false;
  }

  public void init(long cacheSize, ClientCacheContext context) {
    limit = cacheSize;
    mContext =context;
    mContext.stopCache();
  }

  public void fliter(CacheInternalUnit unit, BaseCacheUnit unit1) {
    unit.accessRecord.add(unit1);
    add(unit1);
  }

  public void check(TempCacheUnit unit) {
    long newSize = getNewSize(unit);
    if (newSize + getBlockNum() * blockSize > limit) {
      evict();
    }
    add(unit);
  }

  public void clear() {

  }

  public PolicyName getPolicyName() {
    return PolicyName.TRADITIONAL_LFU;
  }


  public TraditionalLFUEvictor() {
    mVisitQueue = new PriorityQueue<>();
  }
  //0-10 11-20 21-30 31-40 25 35
  public List<Integer> getInvolvedBlock(long begin, long end) {
    long beginIndex = begin / blockSize;
    long endIndex = end /blockSize - 1;
    if (begin % blockSize > 1) {
      beginIndex ++;
    }

    List<Integer> res = new ArrayList<>();
    for (long i = beginIndex; i <= endIndex; i ++) {
      res.add((int)i);
    }
   return res;
  }

  public int getBlockNum() {
    if (visitMap.isEmpty()) return 0;
    int res = 0;
    for( long l : visitMap.keySet()) {
      res += visitMap.get(l).size();
    }
    return res;
  }

  public long evict() {
    long blockNum = getBlockNum();
    long size = blockNum * blockSize;
    long delete = 0;
    while (size > limit) {
     // System.out.println(size / (1024  *1024));
      int minT = -1;
      long minFileId = -1;
      int min = Integer.MAX_VALUE;
      for (long l : visitMap.keySet()) {
        for (int i : visitMap.get(l).keySet()) {
          if (visitMap.get(l).get(i) < min) {
            minT = i;
            min = visitMap.get(l).get(i);
            minFileId = l;
          }
        }
      }
      if (visitMap.containsKey(minFileId)) {
        visitMap.get(minFileId).remove(minT);
        delete += blockSize;
        blockNum --;
        size = blockNum * blockSize;
        removeBlockFromCachaSpace(minFileId, minT);
      }
    }
    return delete;
  }

  public void removeBlockFromCachaSpace(long fileId, int i) {
    CacheUnit unit = mContext.getCache(fileId, mContext.getMetedataCache().getFileLength(fileId), i * blockSize, (i +1) * blockSize, new UnlockTask());
    if(unit.isFinish()) {
      mContext.delete((CacheInternalUnit)unit);
    }
  }

  private void addUnit(long fileId, int i) {
    if (!visitMap.containsKey(fileId)) {
      visitMap.put(fileId, new HashMap<>());
    }
    if (visitMap.get(fileId).containsKey(i)) {
      visitMap.get(fileId).put(i, visitMap.get(fileId).get(i) + 1);
    } else {
      visitMap.get(fileId).put(i, 1);

      mPromoteSize += blockSize;
    }
  }

  public void add(CacheUnit unit) {
    List<Integer> tmp = getInvolvedBlock(unit.getBegin(), unit.getEnd());
    for (int i : tmp) {
      //System.out.println(i);
      addUnit(unit.getFileId(), i);
      addIntoCacheSpace(unit.getFileId(), i);

    }
  }

  public void addIntoCacheSpace(long fileId, int i) {
    CacheUnit unit = mContext.getCache(fileId, mContext.getMetedataCache().getFileLength(fileId), i * blockSize, (i +1) * blockSize, new UnlockTask());
    if (!unit.isFinish()) {
      TempCacheUnit unit1 = (TempCacheUnit) unit;
      try {
        unit1.cache(unit1.getFileId(), (int) (unit1.getEnd() - unit1.getBegin()), mContext.mFileIdToInternalList.get(fileId));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      mContext.addCache(unit1);
    }
  }

  public void init(long limit) throws Exception {
    long sum = 0;
    for(int i = 0 ; i < 1200; i ++) {
      long length = RandomUtils.nextLong(1024 * 8, 1024 * 1024 * 4);
      long begin = RandomUtils.nextLong(0, 1024 * 1024 * 1024 - length);
      sum += length;
      BaseCacheUnit unit = new BaseCacheUnit(mTestFileId, begin, begin + length);
      visitList.add(unit);
      //add(unit);
    }
    System.out.println(visitMap.size() * blockSize / (1024 *1024));
  }

  private long getNewSize(CacheUnit unit) {
    boolean rightLarge, leftSmall;
    List<Integer> tmp = getInvolvedBlock(unit.getBegin(), unit.getEnd());
    if (tmp.size() == 0) {
      rightLarge = leftSmall= false;
    } else {
      rightLarge = unit.getEnd() > (tmp.get(tmp.size() - 1) + 1) * blockSize;
      leftSmall = unit.getBegin() < tmp.get(0) * blockSize + 1;
    }
    long hitSize = 0;
    for (int i : tmp) {
      if (visitMap.containsKey(unit.getFileId()) && visitMap.get(unit.getFileId()).containsKey(i)) {
        hitSize += blockSize;
      }
    }
    if (leftSmall && visitMap.containsKey(unit.getFileId()) && visitMap.get(unit.getFileId()).containsKey(tmp.get(0) - 1)) {
      hitSize += (tmp.get(0) ) * blockSize - unit.getBegin();
      // System.out.println("left : " + (double)((tmp.get(0) ) * blockSize - unit.getBegin()) / (double) (1024 * 1024));
    }
    if (rightLarge && visitMap.containsKey(unit.getFileId()) && visitMap.get(unit.getFileId()).containsKey(tmp.get(tmp.size() - 1) + 1)) {
      hitSize += unit.getEnd() -( tmp.get(tmp.size() - 1)  +1 )* blockSize;
      //System.out.println("right " + (double)(unit.getEnd() -( tmp.get(tmp.size() - 1)  +1 )* blockSize) / (double) (1024 * 1024));
    }
    return unit.getSize() - hitSize;
  }

  public void testVisit() {
    double visitTime = 0;
    for (BaseCacheUnit unit : visitList) {
      mVisitSize += unit.getSize();
      List<Integer> tmp = getInvolvedBlock(unit.getBegin(), unit.getEnd());

      boolean rightLarge, leftSmall;
      if (tmp.size() == 0) {
        rightLarge = leftSmall= false;
      } else {
        rightLarge = unit.getEnd() > (tmp.get(tmp.size() - 1) + 1) * blockSize;
        leftSmall = unit.getBegin() < tmp.get(0) * blockSize + 1;
      }
      for (int i : tmp) {
        if (visitMap.containsKey(unit.getFileId()) && visitMap.get(unit.getFileId()).containsKey(i)) {
          mHitSize += blockSize;
        }
      }
      if (leftSmall && visitMap.containsKey(unit.getFileId()) && visitMap.get(unit.getFileId()).containsKey(tmp.get(0) - 1)) {
        mHitSize += (tmp.get(0) ) * blockSize - unit.getBegin();
       // System.out.println("left : " + (double)((tmp.get(0) ) * blockSize - unit.getBegin()) / (double) (1024 * 1024));
      } else {
        if (tmp.size() > 0 && tmp.get(0) - 1 >= 0)
        addUnit(unit.getFileId(), tmp.get(0) - 1);
      }
      if (rightLarge && visitMap.containsKey(unit.getFileId()) && visitMap.get(unit.getFileId()).containsKey(tmp.get(tmp.size() - 1) + 1)) {
        mHitSize += unit.getEnd() -( tmp.get(tmp.size() - 1)  +1 )* blockSize;
        //System.out.println("right " + (double)(unit.getEnd() -( tmp.get(tmp.size() - 1)  +1 )* blockSize) / (double) (1024 * 1024));
      } else {
        if (tmp.size() > 0 ) {
          addUnit(unit.getFileId(), tmp.get(tmp.size() - 1) + 1);
        }
      }
      add(unit);
      evict();
    }
    System.out.println("hitRatio by size : " + ((double) mHitSize / (double) mVisitSize));
    System.out.println("additional overhead " + mPromoteSize / mVisitSize);
    System.out.println(mPromoteSize/ (1024 * 1024));
    mPromoteSize = 0;
  }

  public static void main(String[] args) throws Exception{
    TraditionalLFUEvictor test = new TraditionalLFUEvictor();
    test.init(1024 * 1024 * 400);
    test.testVisit();
    test.mVisitSize = test.mHitSize = 0;
    test.testVisit();
  }
}
