package alluxio.client.file.cache.submodularLib;

import com.google.common.base.Preconditions;

import java.util.Set;

public abstract class Subgradient<T extends Element> extends IterateOptimizer<T> {

  // public HashSet<Element> mResult = new HashSet<>();
  public Set<T> mChainSet;
  public Set<T> mCandidateSet;
  protected SubmodularCalculator<T> mCalculator;
  protected SubmodularSetUtils<T> mSetUtils;
  public boolean mStopIterate = false;
  public double mMaxSubgradient;

  public int i;

  public Subgradient(SubmodularCalculator<T> calculator, SubmodularSetUtils<T> utils) {
    mCalculator = calculator;
    mSetUtils = utils;
  }

  public void addInputSpace(Set<T> input) {
    mCandidateSet = input;
  }

  @Override
  public void init() {
    mChainSet = null;
    mStopIterate = false;
  }

  public abstract double optimizeObject(T j);

  public abstract boolean iterateLimit(T j);

  @Override
  public boolean convergence() {
    return mStopIterate;
  }


  @Override
  public Set<T> getResult() {
    Preconditions.checkArgument(mStopIterate);
    return mChainSet;
  }

  @Override
  public void iterateOptimize() {
    mMaxSubgradient = -1;
    T result = null;
    mStopIterate = true;
    for (T j : mCandidateSet) {
      if (iterateLimit(j)) {
        mStopIterate = false;
        double tmpSubgradient = optimizeObject(j);
        if (tmpSubgradient > mMaxSubgradient) {
          mMaxSubgradient = tmpSubgradient;
          result = j;
        }
      }
    }
    if (result != null) {
      mChainSet.add(result);
      mCandidateSet.remove(result);
    }
  }
}
