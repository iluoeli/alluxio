package alluxio.client.file.cache.submodularLib.cacheSet;

import alluxio.client.file.cache.core.CacheUnit;
import alluxio.client.file.cache.submodularLib.Subgradient;

import java.util.Iterator;
import java.util.Set;

public class GR extends Subgradient<CacheUnit> {
  CacheHitCalculator mHitCalculator;
  CacheSpaceCalculator mSpaceCalculator;
  double mLimit;

  public GR(long limit, CacheSetUtils utils) {
    super(null, utils);
    mHitCalculator = new CacheHitCalculator(utils);
    mSpaceCalculator = new CacheSpaceCalculator();
    mLimit = limit;
  }

  public void addFileLength(long fileId, long filelength) {
    mHitCalculator.setFileLength(fileId, filelength);
  }

  @Override
  public void addInputSpace(Set<CacheUnit> input) {
    mCandidateSet = input;
    hitValueInit((CacheSet)input);
  }

  @Override
  public boolean iterateLimit(CacheUnit j) {
    if (mHitCalculator.mSpaceSize <= mLimit) {
      return true;
    }
    return false;
  }

  public void hitValueInit(CacheSet set) {
    //Iterator<CacheUnit> iterator = set.iterator();
    //while (iterator.hasNext()) {
    //  mHitCalculator.countHitRatio((BaseCacheUnit)iterator.next());
   // }
  }

  @Override
  public void init() {
    super.init();
    mHitCalculator.mComputeSpace = true;
    mHitCalculator.init();
    mChainSet = new CacheSet();
  }

  @Override
  public double optimizeObject(CacheUnit j) {
    return mHitCalculator.function(j);
  }

  @Override
  public void clear() {
    mCandidateSet.clear();
    mCandidateSet = null;
    mHitCalculator.clear();
    System.gc();
  }

  @Override
  public void iterateOptimize() {
    mMaxSubgradient = 0;
    CacheUnit result = null;
    mStopIterate = true;
    mHitCalculator.iterateInit();
    Iterator<CacheUnit> iter = mCandidateSet.iterator();

    while (iter.hasNext()) {
      CacheUnit j = iter.next();
      double tmpSubgradient = optimizeObject(j);
      if (iterateLimit(j)) {
        if (tmpSubgradient > mMaxSubgradient) {
          mStopIterate = false;
          mMaxSubgradient = tmpSubgradient;
          result = j;
          mHitCalculator.setMaxMark();
        }
      }
      mHitCalculator.backspace();
    }
    if (result != null) {
      mChainSet.add(result);
      mCandidateSet.remove(result);
      if (!mStopIterate) {
        mHitCalculator.addMaxBase(result);
      }
    }
  }
}

