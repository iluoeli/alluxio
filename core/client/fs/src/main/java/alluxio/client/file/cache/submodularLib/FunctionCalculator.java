package alluxio.client.file.cache.submodularLib;

import java.util.Set;

public abstract class FunctionCalculator<T extends Element> {
  protected SubmodularSetUtils mSetUtils;
  protected boolean mAllowCondition;

  public FunctionCalculator(SubmodularSetUtils utils) {
    mSetUtils = utils;
  }

  public abstract double function(Set<T> input);

  public abstract double function(T e);

  public double upperBound(Set<T> input, Set<T> baseSet) {
    Set<T> prevElements = mSetUtils.subtract(baseSet, input);
    Set<T> newElements = mSetUtils.subtract(input, baseSet);
    double needDelete = 0;
    if (!prevElements.isEmpty()) {
      for (Element j : prevElements) {
        Set<T> left = mSetUtils.subtract(baseSet, j);
        Set<T> newUnion = mSetUtils.union(left, j);
        needDelete += (function(newUnion) - function(left));
      }
    }
    double needAdd = 0;
    double emptyValue = 0;
    if (!newElements.isEmpty()) {
      for (T j : newElements) {
        needAdd += (function(j) - emptyValue);
      }
    }
    return function(baseSet) - needDelete + needAdd;
  }

  // public double lowerBound(HashSet<Element> input) {
//
  //}

}
