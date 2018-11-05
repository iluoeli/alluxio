package alluxio.client.file.cache.submodularLib;

import alluxio.client.file.cache.BaseCacheUnit;
import alluxio.client.file.cache.CacheUnit;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSetUtils;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSpaceCalculator;
import com.google.common.base.Preconditions;

import java.util.Set;

public class EASK {
  CacheSpaceCalculator mGx;
  CurveCalcurator mFx;

  public EASK() {
    mGx = new CacheSpaceCalculator();
    mFx = new CurveCalcurator(new CacheSetUtils());
  }


  private class CurveCalcurator extends FunctionCalculator<CacheUnit> {
    private double[] weight;
    private double mCurve = -1;
    private CacheSpaceCalculator mFx = new CacheSpaceCalculator();

    public CurveCalcurator(CacheSetUtils unit) {
      super(unit);
    }

    public void setCurve(double curve) {
      mCurve = curve;
    }

    //TODO init weight vector
    public void initWeight(int spaceSize) {

    }

    public double function(Set<CacheUnit> input) {
      Preconditions.checkArgument(mCurve >= 0 && mCurve <= 1);
      initWeight(input.size());
      CacheSet set = (CacheSet) input;
      BaseCacheUnit unit;
      double sum = 0, sum2 = 0;
      int i = 0;
      while ((unit = set.accessList.poll()) != null) {
        long size = unit.getSize();
        sum += weight[i] * size;
        sum2 += size;
      }
      return Math.sqrt(sum) * mCurve + sum2 * (1 - mCurve);
    }

    public double function(CacheUnit e) {
      long size = e.getSize();
      return Math.sqrt(size) * mCurve + size * (1 - mCurve);
    }
  }
}
