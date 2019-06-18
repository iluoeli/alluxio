package alluxio.client.file.cache.mt.run;

import alluxio.client.file.cache.core.CacheUnit;
import alluxio.client.file.cache.core.ClientCacheContext;
import alluxio.client.file.cache.core.TempCacheUnit;
import alluxio.client.file.cache.core.UnlockTask;
import alluxio.client.file.cache.submodularLib.LRUEvictor;
import alluxio.client.file.cache.mt.run.distributionGenerator.Generator;
import alluxio.client.file.cache.mt.run.distributionGenerator.LRUGenerator;
import alluxio.client.file.cache.mt.run.distributionGenerator.ScanGenerator;
import alluxio.client.file.cache.mt.run.distributionGenerator.ZipfGenerator;
import org.apache.commons.lang3.RandomUtils;

import java.util.*;

public class MTLRUEvictor extends LRUEvictor {
  protected Map<Long, BaseEvictContext> baseEvictCotext = new HashMap<>();
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
  public ClientCacheContext mComputeContext = new ClientCacheContext(false);


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

  public void accessByCount(TmpCacheUnit unit) {
    CacheUnit unit1 = mComputeContext.getCache(unit.getFileId(), mTestFileLength, unit.getBegin(), unit.getEnd(), new UnlockTask());
    if (!unit1.isFinish()) {
      TempCacheUnit unit2 = (TempCacheUnit) unit1;
      mComputeContext.addCache(unit2);
    }
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

  public void testAccess0(int j) {
    int userId = RandomUtils.nextInt(0, 2);
    int tmp;

    tmp = RandomUtils.nextInt(0, 99);
    long fileId;
    if (userId == 0) {
      int accessShare = RandomUtils.nextInt(0,8);
      if (accessShare != 0) {
        fileId = 1;
      } else {
        fileId = 2;
      }
      if (j > 400) {
        if (accessShare != 0) {
          fileId = 2;
        } else {
          fileId = 1;
        }
      }
    }
    else  {
      int accessShare = RandomUtils.nextInt(0, 8);
      if (accessShare == 0) {
        fileId = 1;
      } else {
        fileId = 2;
      }

      if (j > 400) {
        if (accessShare == 0) {
          fileId = 2;
        } else {
          fileId = 1;
        }
      }
    }

    long begin = 1024 * 1024 * tmp;
    long end = begin + 1024 * 1024;
    TmpCacheUnit unit = new TmpCacheUnit(fileId, begin,end);
    access(userId, unit);
    checkSize();

  }


  public void testCheatAccess() {
    cacheSize = 1024 * 1024 * 100;
    mTestFileLength = 1024 * 1024 * 100;
    for (int j = 0; j < 600; j ++) {

      if (j % 3 == 0) {
        evictCheck();
      }
      int userId = RandomUtils.nextInt(0, 2);
      int tmp;

      tmp = RandomUtils.nextInt(0, 99);
      long fileId;

      if (userId ==0) {
        int accessShare = RandomUtils.nextInt(0,5);
        if (accessShare != 0) {
          fileId = 1;
        } else {
          fileId = 2;
        }
        if (j > 400) {
          if (accessShare != 0) {
            fileId = 2;
          } else {
            fileId = 1;
          }
        }
      } else {
        int accessShare = RandomUtils.nextInt(0,5);
        if (accessShare == 0) {
          fileId = 1;
        } else {
          fileId = 2;
        }

        if (j > 400) {
          if (accessShare == 0) {
            fileId = 2;
          } else {
            fileId = 1;
          }
        }
      }

      long begin = 1024 * 1024 * tmp;
      long end = begin + 1024 * 1024;
      TmpCacheUnit unit = new TmpCacheUnit(fileId, begin,end);
      access(userId, unit);
      checkSize();

      if (j % 10 == 0) {
        System.out.println(j + " actual : ");
        double[] tmpArr = new double[3];
        int index = 0;
        for (long userId1 : actualEvictContext.keySet()) {
          System.out.println(userId1 + " : " + actualEvictContext.get(userId1).computePartialHitRatio());
          tmpArr[index ++] = actualEvictContext.get(userId1).computePartialHitRatio();
        }
        //System.out.println(j + " base : ");
       // for (long userId1 : baseEvictCotext.keySet()) {
       //   System.out.println(userId1 + " : " + baseEvictCotext.get(userId1).computePartialHitRatio());
       // }
        System.out.println("global : " + (double) mHitSize / (double) mAccessSize);
        tmpArr[index ++] =  (double) mHitSize / (double) mAccessSize;
        ExcelTest.addnew(j , tmpArr[0], tmpArr[1],tmpArr[2]);
      }




        if (j % 100 == 0 && j <=400) {
          for (long userId1 : actualEvictContext.keySet()) {
            actualEvictContext.get(userId1).initAccessRecord();
          }
          for (long userId1 : baseEvictCotext.keySet()) {
            baseEvictCotext.get(userId1).initAccessRecord();
          }
          mHitSize = mAccessSize = 1;

          if (j == 400) {
          for (int i = 0; i < 10; i++) {
            tmp = RandomUtils.nextInt(0, 99);
            begin = 1024 * 1024 * tmp;
            end = begin + 1024 * 1024;

            cheatAccess(new TmpCacheUnit(2, begin, end), 1);
            cheatAccess(new TmpCacheUnit(2, begin, end), 0);
          }
          System.out.println("cheat begin");
          for (int i = 0; i < 100; i++) {
            tmp = RandomUtils.nextInt(0, 99);
            begin = 1024 * 1024 * tmp;
            end = begin + 1024 * 1024;
            TmpCacheUnit un = new TmpCacheUnit(1, begin, end);
            un.setmAccessTime(2);
            cheatAccess(un, 1);
          }
          System.out.println("cheat end");



         // for (long i : actualEvictContext.keySet()) {
          //  actualEvictContext.get(i).print();
         // }

          for (int i = 0; i < 10; i++) {
            testAccess0(j + 1);
          }
        }
      }
    }
  }

  public void testUserNum_3() {
    ZipfGenerator generator0 = new ZipfGenerator(1023, 0.3);
    ZipfGenerator generator1 = new ZipfGenerator(512, 0.3);
    ZipfGenerator generator2 = new ZipfGenerator(256, 0.3);
      System.out.println("==================================================");
      for (int j = 0; j < 3000; j ++) {
        if (j %3 == 0) {
          evictCheck();
        }
        int tmp;
        int randomIndex = RandomUtils.nextInt(0, 3);
        if (randomIndex < 2) {
          int randomIndex1 = RandomUtils.nextInt(0, 2);
          if (randomIndex1 == 0) {
            randomIndex = 2;
          }
        }
        if (randomIndex % 3 == 0) {
          tmp = RandomUtils.nextInt(0, 1023);
          // tmp = generator0.next();
        } else if (randomIndex % 3 == 1) {
          tmp = RandomUtils.nextInt(0, 512);
          //tmp = generator1.next();
        } else {
          tmp = RandomUtils.nextInt(0, 200);
          //tmp = generator2.next();
        }
        long userId = randomIndex;
        long begin = 1024 * 1024 * tmp;
        long end = begin + 1024 * 1024;
        TmpCacheUnit unit = new TmpCacheUnit(1, begin, end);
        access(userId, unit);
      }
      System.out.println("all : " + (double) mHitSize / (double) mAccessSize);

      System.out.println("actual : ");
      for (long userId1 : actualEvictContext.keySet()) {
        System.out.println(userId1 + " : " + actualEvictContext.get(userId1).computePartialHitRatio());
      }

  }

  public void testUserNum_5() {
    //ZipfGenerator generator0 = new ZipfGenerator(1023, 0.3);
   // ZipfGenerator generator1 = new ZipfGenerator(512, 0.3);
    //ZipfGenerator generator2 = new ZipfGenerator(256, 0.3);

    ZipfGenerator userGenerator = new ZipfGenerator(5, 2);
    for (int i = 0; i < 1; i ++) {
      System.out.println("==================================================");
      for (int j = 0; j < 3000; j ++) {
        if (j %3 == 0) {
          evictCheck();
        }
        int tmp;
        int randomIndex = (5 - userGenerator.next());
        if (randomIndex % 5 == 0) {
          tmp = RandomUtils.nextInt(0, 700);
        } else if (randomIndex % 5 == 1) {
          tmp = RandomUtils.nextInt(0, 650);
        } else if (randomIndex % 5 == 2) {
          tmp = RandomUtils.nextInt(0, 600);
        } else if (randomIndex % 5 == 3) {
          tmp = RandomUtils.nextInt(0, 550);
        }  else  {
          tmp = RandomUtils.nextInt(0, 500);
        }

        long userId = randomIndex;
        long begin = 1024 * 1024 * tmp;
        long end = begin + 1024 * 1024;
        TmpCacheUnit unit = new TmpCacheUnit(1, begin, end);
        access(userId, unit);
        accessByCount(unit);
      }

      System.out.println("all : " + (double) mHitSize / (double) mAccessSize);

      System.out.println("actual : ");
      for (long userId1 : actualEvictContext.keySet()) {
        System.out.println(userId1 + " : " + actualEvictContext.get(userId1).computePartialHitRatio());
      }
      System.out.println("cache ratio :" + (double)cacheSize / (double)mComputeContext.getAllSize(mComputeContext));

    }
    System.out.println(getFairnessIndex());
    System.out.println("fangcha" + getFangcha());
  }

  public double getFangcha() {
    double tmp1 = 700 ;
    double tmp2 = (double) 650 ;
    double tmp3 = (double) 600 ;
    double tmp4 = (double) 550 ;
    double tmp5 = (double) 500 ;

    double ave = (tmp1 + tmp2 + tmp3 + tmp4 + tmp5) / 5;
    double res  = (tmp1 - ave) * (tmp1 - ave) +
            (tmp2 - ave) * (tmp2 - ave) +
    (tmp3 - ave) * (tmp3 - ave) +
    (tmp4 - ave) * (tmp4 - ave) +
    (tmp5 - ave) * (tmp5 - ave);
    return Math.sqrt(res / 5);
  }

  public void testUserNum_10() {
    //ZipfGenerator generator0 = new ZipfGenerator(1023, 0.3);
    // ZipfGenerator generator1 = new ZipfGenerator(512, 0.3);
    //ZipfGenerator generator2 = new ZipfGenerator(256, 0.3);

    ZipfGenerator userGenerator = new ZipfGenerator(10, 1);
    for (int i = 0; i < 1; i ++) {
      System.out.println("==================================================");
      for (int j = 0; j < 3000; j ++) {
        if (j %3 == 0) {
          evictCheck();
        }
        int tmp;
        int randomIndex = (10 - userGenerator.next());
        tmp = RandomUtils.nextInt(0, (10 - randomIndex) * 100);

        long userId = randomIndex;
        long begin = 1024 * 1024 * tmp;
        long end = begin + 1024 * 1024;
        TmpCacheUnit unit = new TmpCacheUnit(1, begin, end);
        access(userId, unit);
      }

      System.out.println("all : " + (double) mHitSize / (double) mAccessSize);

      System.out.println("actual : ");
      for (long userId1 : actualEvictContext.keySet()) {
        System.out.println(userId1 + " : " + actualEvictContext.get(userId1).computePartialHitRatio());
      }
    }
  }


  public void testUserNum_20() {
    //ZipfGenerator generator0 = new ZipfGenerator(1023, 0.3);
    // ZipfGenerator generator1 = new ZipfGenerator(512, 0.3);
    //ZipfGenerator generator2 = new ZipfGenerator(256, 0.3);
    mTestFileLength *= 2;
    cacheSize *= 2;
    ZipfGenerator userGenerator = new ZipfGenerator(20, 0.8);
    for (int i = 0; i < 1; i ++) {
      System.out.println("==================================================");
      for (int j = 0; j < 3000; j ++) {
        int tmp;
        if (j %3 == 0) {
          evictCheck();
        }
        int randomIndex = (20 - userGenerator.next());
        tmp = RandomUtils.nextInt(0, (20 - randomIndex) * 100);


        long userId = randomIndex;
        long begin = 1024 * 1024 * tmp;
        long end = begin + 1024 * 1024;
        TmpCacheUnit unit = new TmpCacheUnit(1, begin, end);
        access(userId, unit);
      }

      System.out.println("all : " + (double) mHitSize / (double) mAccessSize);

      System.out.println("actual : ");
      for (long userId1 : actualEvictContext.keySet()) {
        System.out.println(userId1 + " : " + actualEvictContext.get(userId1).computePartialHitRatio());
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
    long baseNew = baseEvictCotext.get(userId).accessByShare(unit, mContext);
    long actualNew = actualEvictContext.get(userId).access(unit);

    mAccessSize += unit.getSize();
    mHitSize += unit.getSize() - actualNew;
    actualSize += actualNew;
    baseEvictCotext.get(userId).evict();

  }

  public void evict() {
    for (long userId : baseEvictCotext.keySet()) {
      BaseEvictContext mActualContext = actualEvictContext.get(userId);
      LRUEvictContext mBaseContext =(LRUEvictContext) baseEvictCotext.get(userId);
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
    mtlruTest.testUserNum_5();
  }
}
