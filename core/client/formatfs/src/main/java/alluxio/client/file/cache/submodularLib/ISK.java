package alluxio.client.file.cache.submodularLib;

import alluxio.client.file.cache.core.BaseCacheUnit;
import alluxio.client.file.cache.core.CacheUnit;
import alluxio.client.file.cache.submodularLib.cacheSet.*;

import java.util.*;

public class ISK extends IterateOptimizer<CacheUnit> {
  public int iterNum = 0;
  private ISKSubgradient mSubgradient;
  private CacheSet tmpResult;
  private boolean mIsConvergence = false;
  private CacheSet mPreResult;
  public CacheSet mInputSpace;

  public ISK(long limit, CacheSetUtils utils) {
    mSubgradient = new ISKSubgradient(limit, utils);
  }

  public void resetLimit(long limit) {
    mSubgradient.mLimit = limit;
  }

  public void addFileLength(long fileId, long filelength) {
    mSubgradient.mHitCalculator.setFileLength(fileId, filelength);
  }

  @Override
  public void init() {
    tmpResult = new CacheSet();
    mIsConvergence = false;
    mPreResult = new CacheSet();
    iterNum = 0;
  }

  public void addInputSpace(Set<CacheUnit> input) {
    mInputSpace = (CacheSet) input;
    mSubgradient.hitValueInit(mInputSpace);
  }

  @Override
  public CacheSet getResult() {
    return tmpResult;
  }

  @Override
  public boolean convergence() {
    return mIsConvergence;
  }

  @Override
  public void iterateOptimize() {
    mIsConvergence = true;
    mSubgradient.setBaseSet(tmpResult);
    mSubgradient.addInputSpace(mInputSpace.copy());
    mSubgradient.optimize();
    tmpResult = (CacheSet) mSubgradient.getResult();
    mPreResult = tmpResult.copy();
    mSubgradient.clear();
    iterNum++;
    System.out.println(iterNum );
  }

  @Override
  public void clear() {
    mInputSpace.clear();
  }

  public class ISKSubgradient extends Subgradient<CacheUnit> {
    HitCalculator mHitCalculator;
    CacheSpaceCalculator mSpaceCalculator;
    long mLimit;
    HashMap<CacheUnit, Double> mIncrementMap;
    double mBaseFx;
    double mIncrementSum;
    Set<CacheUnit> mBaseSet;
    double newSetVal;
    double mIncrementSumTmp;

    public ISKSubgradient(long limit, CacheSetUtils utils) {
      super(null, utils);
      mHitCalculator = new HitCalculator(null);
      mSpaceCalculator = new CacheSpaceCalculator();
      mLimit = limit;
      mIncrementMap = new HashMap<>();
    }

    public void hitValueInit(CacheSet set) {
      Iterator<CacheUnit> iterator = set.iterator();
      while (iterator.hasNext()) {
        mHitCalculator.countHitRatio((BaseCacheUnit)iterator.next());
      }
    }

    @Override
    public void addInputSpace(Set<CacheUnit> input) {
      mCandidateSet = input;
    }

    @Override
    public boolean iterateLimit(CacheUnit j) {
      double needAdd;
      double needDelete = mIncrementSum;
      if (!mBaseSet.contains(j)) {
        needAdd = newSetVal + (j.getSize());
      } else {
        needAdd = newSetVal;
        needDelete -= mIncrementMap.get(j);
      }
      if ((mBaseFx - needDelete + needAdd) <= mLimit) {
        return true;
      }
      return false;
    }

    public void setBaseSet(Set<CacheUnit> baseSet) {
      mBaseSet = baseSet;
    }

    public void clear() {
      mBaseSet.clear();
      mBaseSet = null;
      mCandidateSet.clear();
      mCandidateSet = null;
      mIncrementMap.clear();
      mHitCalculator.clear();
      System.gc();
    }

    @Override
    public void init() {
      super.init();
      mHitCalculator.init();
      mChainSet = new CacheSet();
      mBaseFx = mSpaceCalculator.function(mBaseSet);
      Iterator<CacheUnit> iter = mBaseSet.iterator();
      mIncrementSum = 0;
      newSetVal = 0;
      mIncrementMap.clear();
      while (iter.hasNext()) {
        CacheUnit tmp = iter.next();
        mPreResult.remove(tmp);
        Double increVal = mBaseFx - mSpaceCalculator.function(mPreResult);
        mPreResult.add(tmp);
        mIncrementMap.put(tmp, increVal);
        mIncrementSum += increVal;
      }
      mIncrementSumTmp = mIncrementSum;
      mHitCalculator.begin = 0;
    }

    @Override
    public double optimizeObject(CacheUnit j) {
      double denominator, needDelete, needAdd = 0;
      if (mBaseSet.contains(j)) {
        needDelete = mIncrementSumTmp - mIncrementMap.get(j);
      } else {
        needDelete = mIncrementSumTmp;
        needAdd = mSpaceCalculator.function(j);
      }
      denominator = mBaseFx - needDelete + needAdd;
      return mHitCalculator.function(j) / denominator;
    }

    @Override
    public void iterateOptimize() {
      mMaxSubgradient = 0;
      CacheUnit result = null;
      mStopIterate = true;
      mHitCalculator.iterateInit();
      Iterator<CacheUnit> iter = ((CacheSet) mCandidateSet).iterator();
      while (iter.hasNext()) {
        CacheUnit j = iter.next();
        if (iterateLimit(j)) {
          double tmpSubgradient = optimizeObject(j);
          if (tmpSubgradient > mMaxSubgradient) {
            mStopIterate = false;
            mMaxSubgradient = tmpSubgradient;
            result = j;
            mHitCalculator.setMaxMark();
          }
          mHitCalculator.backspace();
        }
      }
      if (result != null) {
        if (!mPreResult.contains(result)) {
          mIsConvergence = false;
        }
        mChainSet.add(result);
        if (!mBaseSet.contains(result)) {
          newSetVal += mSpaceCalculator.function(result);
        } else {
          mIncrementSum -= mIncrementMap.get(result);
        }
        (mCandidateSet).remove(result);
        if (!mStopIterate) {
          mHitCalculator.addMaxBase(result);
        }
      }
    }
  }
}
