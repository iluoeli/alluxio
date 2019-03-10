package alluxio.client.file.cache.submodularLib;

import java.util.Set;

public class SubmodularCalculator<T extends Element> {
  private FunctionCalculator<T> mFx;
  private FunctionCalculator<T> mGx;

  public SubmodularCalculator(FunctionCalculator<T> fx, FunctionCalculator<T> gx) {
    mFx = fx;
    mGx = gx;
  }

  public double Fx(Set<T> input) {
    return mFx.function(input);
  }

  public double Gx(Set<T> input) {
    return mGx.function(input);
  }

  public double Fx(T e) {
    return mFx.function(e);
  }

  public double Gx(T e) {
    return mGx.function(e);
  }

  public double FxUpperBound(Set<T> input, Set<T> baseSet) {
    return mFx.upperBound(input, baseSet);
  }

}
