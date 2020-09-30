package alluxio.client.file.cache.core;

import alluxio.client.file.cache.submodularLib.ISK;
import alluxio.client.file.cache.submodularLib.IterateOptimizer;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSetUtils;
import alluxio.client.file.cache.submodularLib.cacheSet.GR;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public enum SKPolicy implements CachePolicy, Runnable {
  INSTANCE;
  public CacheSet mInputSpace1;
  public CacheSet mInputSpace2;
  public volatile boolean useOne = true;
  private volatile boolean isEvicting = false;
  private ClientCacheContext mContext;
  private ClientCacheContext.LockManager mLockManager;
  private long mCacheCapacity;
  private long mCacheSize;
  private double mLowWaterMark = 0.8;
  private double mHighWaterMark = 0.9;
  private IterateOptimizer<CacheUnit> mOptimizer;
  private static long EVICT_THREAD_INTERNAL = 10;
  private final Object mAccessLock = new Object();
  private boolean useGhost;
  private GhostCache mGhost = mContext.getGhostCache();
  private CacheSet newAccessSpace = new CacheSet();
  private ExecutorService mEvictPool = Executors.newSingleThreadExecutor();
  private static int evictContious = 0;
  //private boolean RL = false;
  //private RLAgent mRLAgent;

  public static int testlock = 0;
  private PolicyName mPolicyName;
  private volatile boolean isEvictStart;

  public void setPolicy(PolicyName name) {
    if (name == PolicyName.ISK) {
      mOptimizer = null;
      mOptimizer = new ISK((long) (mCacheCapacity * mLowWaterMark), new CacheSetUtils());
      mPolicyName = PolicyName.ISK;
    } else if (name == PolicyName.GR) {
      mOptimizer = null;
      mOptimizer = new GR((long) (mCacheCapacity * mLowWaterMark), new CacheSetUtils());
      mPolicyName = PolicyName.GR;
    }
  }
  @Override
  public boolean isFixedLength() {
    return true;
  }

  @Override
  public PolicyName getPolicyName() {
    return mPolicyName;
  }

  public void useGhost() {
    mContext.mUseGhostCache = true;
    useGhost = true;
  }

  public CacheSet move(CacheSet s1, CacheSet s2) {
    for (long fileId : s2.cacheMap.keySet()) {
      Set<CacheUnit> s = s2.get(fileId);
      if (!s1.cacheMap.containsKey(fileId)) {
        s1.cacheMap.put(fileId, new TreeSet<>(new Comparator<CacheUnit>() {
          @Override
          public int compare(CacheUnit o1, CacheUnit o2) {
            return 0;
          }
        }));
      }
      s1.get(fileId).addAll(s);
    }
    s2.clear();
    return s1;
  }

  public long evict() {
    System.out.println("evict begin " + mCacheSize / (1024 * 1024) + " " + mCacheCapacity / (1024 * 1024) + " " + Thread.currentThread().getId());
    isEvicting = true;
    synchronized (mAccessLock) {
      if (useOne) {
        mOptimizer.addInputSpace(mInputSpace1);
        System.out.println(mInputSpace1.size());
      } else {
        mOptimizer.addInputSpace(mInputSpace2);
        System.out.println(mInputSpace2.size());
      }
      useOne = !useOne;
    }
    mOptimizer.optimize();
    CacheSet result = (CacheSet) mOptimizer.getResult();
    mOptimizer.clear();
    mLockManager.evictStart();
    long delete = 0;
    mContext.stopCache();
    try {
      synchronized (mAccessLock) {
        if (!useOne) {
          move(mInputSpace2, result);
          result = mInputSpace2;
        } else {
          move(mInputSpace1, result);
          result = mInputSpace1;
        }
        if (useGhost) {
          mGhost.clear();
        }
      }
      for (long fileId : result.keySet()) {
        Set<CacheUnit> inputSet = result.get(fileId);
        delete += mContext.mFileIdToInternalList.get(fileId).elimiate(inputSet);
      }
      mContext.allowCache();

      mCacheSize -= delete;
      System.out.println("evict end " + mCacheSize);
      isEvicting = false;
      testlock++;
    } finally {
      mLockManager.evictEnd();
    }
    return delete;
  }

  private synchronized boolean evictCheck() {
    if (mCacheSize > mHighWaterMark * mCacheCapacity) {
      return true;
    }
    return false;
  }

  private boolean LowHitRatioCheck() {
    if (useGhost) {
      synchronized (mAccessLock) {
        CacheSet input;
        if (useOne) {
          input = mInputSpace1;
        } else {
          input = mInputSpace2;
        }
        double hitrate = mGhost.statisticsHitRatio(input);
        //System.out.println(hitrate);
        if ((hitrate / (double) input.size()) > mGhost.mGhostHit) {
          return true;
        } else {
          return false;
        }
      }
    } else {
      return false;
    }
  }

  @Override
  public void run() {
    useGhost();
    System.out.println("check thread " + Thread.currentThread().getId());
    while (true) {
      try {
        if (evictCheck()) {
          if (!isEvicting) {
            mEvictPool.submit(new Runnable() {
              @Override
              public void run() {
                try {
                  evict();
                } catch (Exception e) {
                  e.printStackTrace();
                } finally {
                  if (!mContext.isAllowCache()) {
                    mContext.allowCache();
                  }
                  if (evictCheck()) {
                    evictContious ++;
                  } else {
                    evictContious = 0;
                  }
                }
              }
            });
          }
        }
        if (mCacheSize > mCacheCapacity || evictContious >= 2) {
          if (mCacheSize > mCacheCapacity && mContext.isAllowCache()) {
            mContext.stopCache();
          }
          if (mHighWaterMark >= 0.8 || mLowWaterMark <= 0.2) {
            continue;
          }

          //if(LowHitRatioCheck()) {
          //	mLowWaterMark += 0.1;
          //	if(mHighWaterMark - mLowWaterMark <= 0.2) {
          ///		mHighWaterMark += 0.1;
          //	}
          //} else {
          // mHighWaterMark -= 0.1;
          //	if(mHighWaterMark - mLowWaterMark <= 0.2) {
          //	mLowWaterMark -= 0.1;
          //	}
          //	}
          //	System.out.println("===========" +mHighWaterMark+ " "
          // +mLowWaterMark);
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        try {
          Thread.sleep(EVICT_THREAD_INTERNAL);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public void fliter(CacheInternalUnit unit, BaseCacheUnit unit1) {
    synchronized (mAccessLock) {
      if (useOne) {
        mInputSpace1.add(unit1);
        mInputSpace1.addSort(unit1);
      } else {
        mInputSpace2.add(unit1);
        mInputSpace2.addSort(unit1);
      }
    }
    if (useGhost) {
      newAccessSpace.add(unit1);
    }
  }

  public boolean isSync() {
    return false;
  }

  @Override
  public void init(long cacheSize, ClientCacheContext context) {
    mCacheSize = 0;
    mCacheCapacity = cacheSize;
    mContext = context;
    setPolicy(PolicyName.ISK);
    mLockManager = mContext.getLockManager();
    mInputSpace1 = new CacheSet();
    mInputSpace2 = new CacheSet();

    if (!isEvictStart) {
      synchronized (this) {
        if (!isEvictStart) {
          mContext.COMPUTE_POOL.submit(this);
          isEvictStart = true;
        }
      }
    }
  }

  public void check(TempCacheUnit unit) {
    mCacheSize += unit.getNewCacheSize();
    // if(RL) {
    //  mRLAgent.AddCurrentReword(unit.getRealReadSize(), unit.getNewCacheSize());
    //}
  }

  @Override
  public void clear() {

  }
}
