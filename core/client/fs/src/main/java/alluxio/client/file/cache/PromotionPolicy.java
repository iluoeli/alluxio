package alluxio.client.file.cache;

import alluxio.client.file.cache.submodularLib.ISK;
import alluxio.client.file.cache.submodularLib.IterateOptimizer;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSetUtils;
import alluxio.client.file.cache.submodularLib.cacheSet.GR;
import alluxio.exception.AlluxioException;
import alluxio.network.netty.NettyRPC;

import javax.swing.event.CaretListener;
import java.io.IOException;
import java.util.*;

import static alluxio.client.file.cache.ClientCacheContext.mPromotionThreadId;
import static alluxio.client.file.cache.ClientCacheContext.useNettyMemoryUtils;

public class PromotionPolicy implements Runnable {
  private long mCacheCapacity;
  private IterateOptimizer<CacheUnit> mOptimizer;
  public CacheSet mInputSpace1;
  public CacheSet mInputSpace2;
  public volatile boolean useOne = true;
  public boolean isProtomoting = false;
  private final Object mAccessLock = new Object();
  private ClientCacheContext mContext = ClientCacheContext.INSTANCE;
  private volatile int mSize;
  private boolean onlyOne = false;

  public void setPolicy(CachePolicy.PolicyName name) {
    if (name == CachePolicy.PolicyName.ISK) {
      mOptimizer = null;
      mOptimizer = new ISK((mCacheCapacity), new CacheSetUtils());
    } else if (name == CachePolicy.PolicyName.GR) {
      mOptimizer = null;
      mOptimizer = new GR((mCacheCapacity), new CacheSetUtils());
    }
  }

  public void addFileLength(long fileId, long fileLength) {
    if (mOptimizer instanceof GR) {
      GR op = (GR)mOptimizer;
      op.addFileLength(fileId, fileLength);
    } else if (mOptimizer instanceof ISK) {
      ISK op = (ISK)mOptimizer;
      op.addFileLength(fileId, fileLength);
    }
  }

  public void filter(BaseCacheUnit unit1) {
    addFileLength(unit1.getFileId(), ClientCacheContext.INSTANCE.getMetedataCache().getFileLength
      (unit1.getFileId()));
    synchronized (mAccessLock) {
      if (useOne) {
        if (mSize > 2500) {
          mInputSpace1.clear();
        }
        mInputSpace1.add(unit1);
        mInputSpace1.addSort(unit1);
      } else {
        if (mSize > 2500) {
          mInputSpace2.clear();
        }
        mInputSpace2.add(unit1);
        mInputSpace2.addSort(unit1);
      }
      mSize++;
    }

  }

  public void updateTest(ClientCacheContext context) {
    System.out.println("start update");
    isProtomoting = true;
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
    synchronized (mAccessLock) {
      promoteReset();
    }
    try {
      result.convertSort();
      System.out.println("start merge");
      mContext.mergeTest(result, context);
    } catch (IOException | AlluxioException e) {
      throw new RuntimeException(e);
    }
    System.out.println("update finish");
  }

  public void update() {
    System.out.println("start update");
    isProtomoting = true;
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
    synchronized (mAccessLock) {
      promoteReset();
    }
    try {
      result.convertSort();
      System.out.println("start merge");
      mContext.merge(result);
    } catch (IOException | AlluxioException e) {
      throw new RuntimeException(e);
    }
    System.out.println("update finish");
  }

  private void promoteReset() {
    mSize = 0;
  }

  private boolean promoteCheck() {
    return mSize > 1100;
  }

  public void init(long limit) {
    mCacheCapacity = limit;
    mInputSpace1 = new CacheSet();
    mInputSpace2 = new CacheSet();
    setPolicy(CachePolicy.PolicyName.GR);
    mContext.stopCache();
    mContext.COMPUTE_POOL.submit(this);
  }


  @Override
  public void run() {
    mPromotionThreadId = Thread.currentThread().getId();
    System.out.println("promoter begins to run");

    while (true) {
      try {
        if (promoteCheck()) {
         // if (!onlyOne) {
            update();
            onlyOne = true;
         // } else {

         // }
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
