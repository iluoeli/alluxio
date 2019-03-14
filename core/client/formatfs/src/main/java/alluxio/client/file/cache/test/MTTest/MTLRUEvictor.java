package alluxio.client.file.cache.test.MTTest;

import alluxio.client.file.cache.*;
import alluxio.client.file.cache.test.LRUEvictor;
import org.apache.commons.lang3.RandomUtils;

import java.util.*;

public class MTLRUEvictor extends LRUEvictor {
  protected Map<Long, LRUEvictContext> baseEvictCotext = new HashMap<>();
  public Map<Long, LRUEvictContext> actualEvictContext = new HashMap<>();
  private List<Double> mVCValue = new ArrayList<>();
  protected int mCurrentIndex = 0;
  private double mEvictRatio = 0.2;
  private int sampleSize = 1000;
  protected long actualSize = 0;
  protected long mAccessSize;
  protected long mHitSize;
  public long mLimit = 10 * 1024 * 1024;
  public BaseEvictContext mBaseEvictContext = new LRUEvictContext(this, new ClientCacheContext(false), -1);

  public void checkRemoveByShare(TmpCacheUnit unit, long userID) {
    for (long userId : actualEvictContext.keySet()) {
      if (userID != userId)
      actualEvictContext.get(userId).removeByShare(unit);

    }
  }

  public MTLRUEvictor(ClientCacheContext context) {
    super(context);

  }

  public double function(double input) {
    //System.out.println(input);
    return input * 10;
  }

  public List<TmpCacheUnit> sample(int l) {
    List<TmpCacheUnit> res = new ArrayList<>();
    
    for (long fileId : actualEvictContext.keySet()) {
      List<TmpCacheUnit> tmp = actualEvictContext.get(fileId).getCacheList();
      for (TmpCacheUnit t : tmp) {
        if (res.size() < l) {
          res.add(t);
        } else {
          int tmpIndex = RandomUtils.nextInt(0, l - 1);
          res.add(tmpIndex, t);
        }
      }
    }

    res.sort( new Comparator<TmpCacheUnit>() {
      @Override
      public int compare(TmpCacheUnit o1, TmpCacheUnit o2) {
        if (o1.mCost == o2.mCost) return 0;
        else if (o1.mCost > o2.mCost) return 1;
        else return -1;
      }
    });
    return res;
  }

  public void test() {
    for (int i = 0; i < 20; i ++) {
      System.out.println("==================================================");
     // mAccessSize = mHitSize = 0;
      if (i == 10) {
        for (long userId : actualEvictContext.keySet()) {
         actualEvictContext.get(userId).initAccessRecord();
        }
        for (long userId : baseEvictCotext.keySet()) {
          baseEvictCotext.get(userId).initAccessRecord();
        }
      }
      mBaseEvictContext.initAccessRecord();
      boolean reverse = false;
      for (int j = 0; j < 1000; j ++) {
        if (j % 3 == 0) {
          evictCheck();
        }
       int randomIndex = RandomUtils.nextInt(0, 3);
       if (randomIndex < 2) {
         int randomIndex1 = RandomUtils.nextInt(0, 2);
         if (randomIndex1 == 0) {
           randomIndex = 2;
         }
        }
        int tmp;
        if (randomIndex == 0) {
          tmp = RandomUtils.nextInt(0, 1023);
        } else if (randomIndex == 1) {
          tmp = RandomUtils.nextInt(0, 512);
        } else {
          tmp = RandomUtils.nextInt(0, 256);
        }
        //long userId = RandomUtils.nextInt(0, 10);
       // if (userId >= 2) {
       //   int tmp1 = RandomUtils.nextInt(0, 10);
       //   if (tmp1 != 0) {
       //     userId = RandomUtils.nextInt(0, 5);
       //   }
       // }
        long userId = randomIndex;
        long begin = 1024 * 1024 * tmp;
        long end = begin + 1024 * 1024;
        TmpCacheUnit unit = new TmpCacheUnit(mTestFileId, begin, end);
        access(userId, unit);
      }
      /*
      for (int j = 0; j < 3072; j ++) {
        if (j % 3 == 0) {
          evictCheck();
        }
        int tmp = RandomUtils.nextInt(0, 512);
        long userId = RandomUtils.nextInt(0, 3);

        long begin = 1024 * 1024 * tmp;
        long end = begin + 1024 * 1024;
        TmpCacheUnit unit = new TmpCacheUnit(mTestFileId, begin, end);
        access(userId, unit);
      }
     */
      System.out.println("all : " + (double)mHitSize / (double)mAccessSize);
      System.out.println("actual : ");
      for (long userId : actualEvictContext.keySet()) {
        System.out.println(userId + " : " + actualEvictContext.get(userId).computePartialHitRatio());
      }
      System.out.println("base : ");
      for (long userId : baseEvictCotext.keySet()) {
        System.out.println(userId + " : " + baseEvictCotext.get(userId).computePartialHitRatio());
      }

      System.out.println("only one : ");
      System.out.println(mBaseEvictContext.computePartialHitRatio());


    }
  }

  private void evictCheck() {
   // double before = mEvictRatio;
    while (actualSize > cacheSize ) {
      evict();
      //System.out.println(actualSize+ " ----- " + cacheSize);
    }
    // mEvictRatio = before;
  }

  public void access(long userId, TmpCacheUnit unit) {
    unit.mClientIndex = mCurrentIndex;
    if (!baseEvictCotext.containsKey(userId)) {
      LRUEvictContext base = new LRUEvictContext(this, new ClientCacheContext(false), userId);
      base.resetCapacity((long)(cacheSize));
      baseEvictCotext.put(userId, base);
      actualEvictContext.put(userId, new LRUEvictContext(this, mContext, userId));
    }
    long baseNew = baseEvictCotext.get(userId).accessByShare(unit, actualEvictContext.get(userId).mCacheContext);
    long actualNew = actualEvictContext.get(userId).access(unit);

    mAccessSize += unit.getSize();
    mHitSize += unit.getSize() - actualNew;
    actualSize += actualNew;
    baseEvictCotext.get(userId).evict();

  }

  private void evict() {
    for (long userId : baseEvictCotext.keySet()) {
      LRUEvictContext mActualContext = actualEvictContext.get(userId);
      LRUEvictContext mBaseContext = baseEvictCotext.get(userId);
      double HRD = function(mBaseContext.computePartialHitRatio() - mActualContext.computePartialHitRatio());
      //System.out.println(HRD);
      evictForEachUser(HRD, (LinkedList)mActualContext.getCacheList());
    }

    List<TmpCacheUnit> sampleRes = sample(sampleSize);
    double midCost = sampleRes.get((int)(mEvictRatio * sampleRes.size())).mCost;
    for (long userID : actualEvictContext.keySet()) {

      LRUEvictContext mContext = actualEvictContext.get(userID);
      double nearstGap = Integer.MAX_VALUE;
      double midValue = 0;
      for (TmpCacheUnit unit : mContext.getCacheList()) {
        if (Math.abs(unit.mCost - midCost) < nearstGap) {
          nearstGap = Math.abs(unit.mCost - midCost);
          midValue = unit.mCost;
        }
      }
      Set<TmpCacheUnit> deleteSet = new HashSet<>();
      for (TmpCacheUnit unit : mContext.getCacheList()) {
        if (unit.mCost < midValue) {
          deleteSet.add(unit);
        }
      }
      for (TmpCacheUnit deleteUnit : deleteSet) {
        actualSize -= actualEvictContext.get(userID).remove(deleteUnit);
      }
    }

    mVCValue.add(midCost);
    mCurrentIndex ++;
  }

  private void evictForEachUser(double HRD, LinkedList<TmpCacheUnit> LRUList) {
    long leastIndex = LRUList.getFirst().mClientIndex;
    double avc = 0;
    for (long i = leastIndex; i < mCurrentIndex && i < LRUList.size(); i ++) {
      avc += mVCValue.get((int)i);
    }
    for (int i = 0 ; i < LRUList.size(); i ++) {
      LRUList.get(i).mCost = HRD - avc + avc * ((double) i/(double) LRUList.size() -1);
    }
  }

  public static void main(String [] args) {
    MTLRUEvictor mtlruTest = new MTLRUEvictor(new ClientCacheContext(false));
    mtlruTest.test();
  }
}
