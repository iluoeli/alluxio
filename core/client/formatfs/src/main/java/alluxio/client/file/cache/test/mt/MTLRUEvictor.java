package alluxio.client.file.cache.test.mt;

import alluxio.client.file.cache.*;
import alluxio.client.file.cache.test.LRUEvictor;
import alluxio.client.file.cache.test.mt.distributionGenerator.Generator;
import alluxio.client.file.cache.test.mt.distributionGenerator.LRUGenerator;
import alluxio.client.file.cache.test.mt.distributionGenerator.ScanGenerator;
import alluxio.client.file.cache.test.mt.distributionGenerator.ZipfGenerator;
import org.apache.commons.lang3.RandomUtils;

import java.util.*;

public class MTLRUEvictor extends LRUEvictor {
  protected Map<Long, LRUEvictContext> baseEvictCotext = new HashMap<>();
  public Map<Long, BaseEvictContext> actualEvictContext = new HashMap<>();
  private List<Double> mVCValue = new ArrayList<>();
  protected int mCurrentIndex = 0;
  private double mEvictRatio = 0.5;
  private int sampleSize = 1000;
  protected long actualSize = 0;
  protected long mAccessSize;
  protected long mHitSize;
  public long mLimit = 10 * 1024 * 1024;
  private int userNum = 3;
  public long mShareVisitSize = 0;
  public BaseEvictContext mBaseEvictContext = new LRUEvictContext(this, new ClientCacheContext(false), -1);

  public static Map<Long, Generator> userMap = new HashMap<>();
  public Map<TmpCacheUnit, Set<Long>> mShareSet = new HashMap<>();
  public List<TmpCacheUnit> mAccessCollecter = new LinkedList<>();

  static {
    userMap.put(1L, new LRUGenerator(1000));
    userMap.put(2L, new ZipfGenerator(1000, 0.3));
    userMap.put(3L, new ScanGenerator(1000));
  }

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

  public void cheatAccess(TmpCacheUnit unit, long userId) {
    long actualNew = actualEvictContext.get(userId).cheatAccess(unit);
    if (actualNew > 0) {
      actualSize += actualNew;
      while (actualSize > cacheSize) {
        evict();
      }
    }
    //mBaseEvictContext.cheatAccess(unit);
    //mBaseEvictContext.evict();
  }


  private double getFairnessIndex() {
    double tmpSum = 0;
    double tmpSum2 = 0;
    for (long userId : baseEvictCotext.keySet()) {
      double baseHitRatio = baseEvictCotext.get(userId).computePartialHitRatio();
      double actualRatio = actualEvictContext.get(userId).computePartialHitRatio();
      double accessRatio = (double) actualEvictContext.get(userId).mVisitSize / (double) mAccessSize;
      double tmpVal =(actualRatio / (baseHitRatio / userNum )) / accessRatio;
      tmpSum += tmpVal;
      tmpSum2 += tmpVal * tmpVal;
    }
    tmpSum = tmpSum * tmpSum;

    return tmpSum / (userNum * tmpSum2);

  }

  public void checkSize() {
    long res = 0;
    for (long id : actualEvictContext.keySet()) {
      res += actualEvictContext.get(id).mCacheSize;
    }
    if (res != actualSize) {
      System.out.println(res + " " + actualSize);
      throw new RuntimeException();
    }
  }

  public void testCheatAccess() {
    for (int j = 0; j < 500; j ++) {

      if (j % 3 == 0) {
        evictCheck();
      }

      int userId = RandomUtils.nextInt(0, 2);
      int tmp;

      tmp = RandomUtils.nextInt(0, 99);
      long fileId = userId;

      if (userId ==0) {
        int accessShare = RandomUtils.nextInt(0,2);
        if (accessShare == 0) {
          fileId = 2;
        }
        if (j > 400) {
          //fileId = 4;
          accessShare = RandomUtils.nextInt(0,10);
          if (accessShare == 0) {
            fileId = 3;
          }
        }
      } else {
        int accessShare = RandomUtils.nextInt(0,2);
        if (accessShare != 0) {
          fileId = 2;
        }
        if (j > 400) {
          fileId = 3;
        }
      }

      long begin = 1024 * 1024 * tmp;
      long end = begin + 1024 * 1024;
      TmpCacheUnit unit = new TmpCacheUnit(fileId, begin,end);
      access(userId, unit);
      checkSize();

      if (j % 10 == 0) {
        System.out.println(j + " actual : ");
        for (long userId1 : actualEvictContext.keySet()) {
          System.out.println(userId1 + " : " + actualEvictContext.get(userId1).computePartialHitRatio());
        }
        System.out.println("global : " + (double) mHitSize / (double) mAccessSize);
      }

      if (j == 400) {
        //user 1 cheat;
        for (int i = 0; i < 1000; i ++) {
          tmp = RandomUtils.nextInt(0, 99);
          begin = 1024 * 1024 * tmp;
          end = begin + 1024 * 1024;
          cheatAccess(new TmpCacheUnit(3, begin, end), 1);
        }
        if (j % 100 ==0) {
          for (long userId1 : actualEvictContext.keySet()) {
            actualEvictContext.get(userId1).initAccessRecord();
          }
          for (long userId1 : baseEvictCotext.keySet()) {
            baseEvictCotext.get(userId1).initAccessRecord();
          }
        }
      }
    }
  }

  public void test() {
    ZipfGenerator generator0 = new ZipfGenerator(1000, 0.3);
    ZipfGenerator generator1 = new ZipfGenerator(512, 0.3);
    ZipfGenerator generator2 = new ZipfGenerator(256, 0.3);
    //LRUGenerator generator0 = new LRUGenerator(1024);
    //LRUGenerator generator1 = new LRUGenerator(512);
   // LRUGenerator generator2 = new LRUGenerator(256);
    ZipfGenerator userGenerator = new ZipfGenerator(3, 1);
    ZipfGenerator fileIDGenerator = new ZipfGenerator(3, 9);
    LRUGenerator generator = new LRUGenerator(200);
    for (int i = 0; i < 1; i ++) {
      System.out.println("==================================================");
      mAccessCollecter.clear();
      mShareSet.clear();
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

        //  randomIndex = userGenerator.next() +1;
        randomIndex = RandomUtils.nextInt(0, 2);
        int tmp;
        //userMap.get((long)randomIndex).next();

        if (randomIndex % 3 == 0) {
          tmp = RandomUtils.nextInt(0, 200);
          // tmp = generator0.next();
        } else if (randomIndex % 3 == 1) {
          tmp = RandomUtils.nextInt(0, 200);
          //tmp = generator1.next();
        } else {
          tmp = RandomUtils.nextInt(0, 200);
          //tmp = generator2.next();
        }
         tmp = generator.next();
        //tmp = RandomUtils.nextInt(0, 1000);
        //long userId =
        // if (userId >= 2) {
        //   int tmp1 = RandomUtils.nextInt(0, 10);
        //   if (tmp1 != 0) {
        //     userId = RandomUtils.nextInt(0, 5);
        //   }
        // }
        long userId = randomIndex;
        long begin = 1024 * 1024 * tmp;
        long end = begin + 1024 * 1024;
        long TestFileId = fileIDGenerator.next();
        TmpCacheUnit unit = new TmpCacheUnit(1, begin, end);
        access(userId, unit);

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
        if (j % 10 == 0) {
          //System.out.println("all : " + (double) mHitSize / (double) mAccessSize);

          System.out.println("actual : ");
          for (long userId1 : actualEvictContext.keySet()) {
            System.out.println(userId1 + " : " + actualEvictContext.get(userId1).computePartialHitRatio());
          }
          /*
          System.out.println("base : ");
          for (long userId1 : baseEvictCotext.keySet()) {
            System.out.println(userId1 + " : " + baseEvictCotext.get(userId1).computePartialHitRatio());
          }

          System.out.println("only one : ");
          System.out.println(mBaseEvictContext.computePartialHitRatio());
          //System.out.println("access share ratio : " + (double)mShareVisitSize / (double)mAccessSize );
          long shareSize = 0;
          for (TmpCacheUnit u : mShareSet.keySet()) {
            Set<Long> s = mShareSet.get(u);
            if (s.size() > 1) {
              shareSize++;
            }
          }
          System.out.println("storage share ratio : " + (double) shareSize / (double) mShareSet.size());
          shareSize = 0;
          long acc = 0;
          for (TmpCacheUnit uu : mAccessCollecter) {
            if (mShareSet.get(uu).size() > 1) {
              shareSize += uu.getSize();
            }
            acc += uu.getSize();
          }

          System.out.println("visit share ratio :" + (double) shareSize / (double) acc);

          System.out.println(getFairnessIndex());*/
        }
      }
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

  public void evict() {
    for (long userId : baseEvictCotext.keySet()) {
      BaseEvictContext mActualContext = actualEvictContext.get(userId);
      LRUEvictContext mBaseContext = baseEvictCotext.get(userId);
      double HRD = function(mBaseContext.computePartialHitRatio() - mActualContext.computePartialHitRatio());
      //System.out.println(HRD);
      evictForEachUser(HRD, (LinkedList)mActualContext.getCacheList());
    }

    List<TmpCacheUnit> sampleRes = sample(sampleSize);
    double midCost = sampleRes.get((int)(mEvictRatio * sampleRes.size())).mCost;
    for (long userID : actualEvictContext.keySet()) {

      BaseEvictContext mContext = actualEvictContext.get(userID);
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
