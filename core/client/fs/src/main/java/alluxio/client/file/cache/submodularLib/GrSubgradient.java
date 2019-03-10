package alluxio.client.file.cache.submodularLib;

import java.util.Set;

public class GrSubgradient extends Subgradient {
  public double b;
  public double mPrevGx;

  public GrSubgradient(double limit, SubmodularCalculator calculator, SubmodularSetUtils utils) {
    super(calculator, utils);
    b = limit;
  }

  public void clear() {

  }

  @Override
  public void init() {
    super.init();
    mPrevGx = 0;
  }

  @Override
  public double optimizeObject(Element j) {
    double result;
    Set<Element> tmp = mSetUtils.union(mChainSet, j);
    if (mPrevGx == 0) {
      mPrevGx = mCalculator.Gx(mChainSet);
      result = mCalculator.Gx(tmp) - mPrevGx;
    } else {
      double mNowGx = mCalculator.Gx(tmp);
      result = mNowGx - mPrevGx;
      // mPrevGx = mNowGx;
    }
    return result;
  }

  @Override
  public boolean iterateLimit(Element j) {
    if (mCalculator.Fx(mSetUtils.union(mChainSet, j)) <= b) {
      return true;
    }
    return false;
  }
}
