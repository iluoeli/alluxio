package file.cache.test;

import file.cache.BaseCacheUnit;
import file.cache.CacheUnit;
import file.cache.ClientCacheContext;
import file.cache.UnlockTask;
import file.cache.submodularLib.cacheSet.CacheSet;
import file.cache.test.mt.MTLRUEvictor;
import file.cache.test.mt.TmpCacheUnit;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.SystemUtils;

import java.util.*;

public class LFUEvictor extends LRUEvictor {
  public PriorityQueue<TmpCacheUnit> mVisitQueue;
  private Map<Integer, Integer> visitMap = new HashMap<>();
  public long blockSize = (long) (1024 * 1024 * 2 );
  private double mVisitSize = 0;
  private double mHitSize = 0;
  public long limit = 1024 * 1024 * 400;
  public long mPromoteSize = 0;

  public LFUEvictor(ClientCacheContext cacheContext) {
    super(cacheContext);
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
   for (long i = beginIndex; i <=endIndex; i ++) {
     res.add((int)i);
   }
   return res;
  }

  public void evict() {
    long size = visitMap.size() * blockSize;
    while (size > limit) {
     // System.out.println(size / (1024  *1024));
      int minT = -1;
      int min = Integer.MAX_VALUE;
      for (int i : visitMap.keySet()) {
        if (visitMap.get(i) < min) {
          minT = i;
          min = visitMap.get(i);
        }
      }
      visitMap.remove(minT);
      size = visitMap.size() * blockSize;
    }
  }

  private void addUnit(int i) {
    if (visitMap.containsKey(i)) {
      visitMap.put(i, visitMap.get(i) + 1);
    } else {
      visitMap.put(i, 1);
      mPromoteSize += blockSize;
    }
  }

  public void add(BaseCacheUnit unit) {
    List<Integer> tmp = getInvolvedBlock(unit.getBegin(), unit.getEnd());

    for (int i : tmp) {
      //System.out.println(i);
      addUnit(i);
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
        if (visitMap.containsKey(i)) {
          mHitSize += blockSize;
        }
      }
      if (leftSmall && visitMap.containsKey(tmp.get(0) - 1)) {
        mHitSize += (tmp.get(0) ) * blockSize - unit.getBegin();
       // System.out.println("left : " + (double)((tmp.get(0) ) * blockSize - unit.getBegin()) / (double) (1024 * 1024));
      } else {
        if (tmp.size() > 0 && tmp.get(0) - 1 >= 0)
        addUnit(tmp.get(0) - 1);
      }
      if (rightLarge && visitMap.containsKey(tmp.get(tmp.size() - 1) + 1)) {
        mHitSize += unit.getEnd() -( tmp.get(tmp.size() - 1)  +1 )* blockSize;
        //System.out.println("right " + (double)(unit.getEnd() -( tmp.get(tmp.size() - 1)  +1 )* blockSize) / (double) (1024 * 1024));
      } else {
        if (tmp.size() > 0 ) {
          addUnit(tmp.get(tmp.size() - 1) + 1);
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
    LFUEvictor test = new LFUEvictor(new ClientCacheContext(false));
    test.init(1024 * 1024 * 400);
    test.testVisit();
    test.mVisitSize = test.mHitSize = 0;
    test.testVisit();


  }
}
