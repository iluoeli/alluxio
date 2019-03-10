package alluxio.client.file.cache.submodularLib.cacheSet;

import alluxio.client.file.cache.*;
import alluxio.client.file.cache.struct.DoubleLinkedList;
import alluxio.client.file.cache.submodularLib.FunctionCalculator;
import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * This class only handle set which type is CacheSet and element is CacheUnit
 */
public class CacheSpaceCalculator extends FunctionCalculator<CacheUnit> {

  public CacheSpaceCalculator() {
    super(new CacheSetUtils());
  }

  @Override
  public double function(Set<CacheUnit> input) {
    Preconditions.checkArgument(input instanceof CacheSet);
    CacheSet tmpSet = (CacheSet) input;
    tmpSet.sortConvert();
    if (input.isEmpty()) {
      return 0;
    }
    CacheSet set = (CacheSet) input;
    double res = 0;
    for (Map.Entry entry : set.sortCacheMap.entrySet()) {
      res += function0((PriorityQueue<CacheUnit>) entry.getValue());
    }
    return res;

  }

  private double function0(PriorityQueue<CacheUnit> queue) {
    int res = 0;
    long begin = 0;
    long maxEnd = 0;
    while (!queue.isEmpty()) {
      CacheUnit unit = queue.poll();
      if (unit.getBegin() >= maxEnd) {
        res += (maxEnd - begin);
        begin = unit.getBegin();
        maxEnd = unit.getEnd();
      } else {
        maxEnd = Math.max(unit.getEnd(), maxEnd);
      }
    }
    res += (maxEnd - begin);
    return res;
  }

  private double function0(Set<CacheUnit> set) {
    double res = 0;

    Iterator<CacheUnit> iter = set.iterator();
    DoubleLinkedList<CacheInternalUnit> tmpUnit = new DoubleLinkedList<>(new CacheInternalUnit(0, 0, -1));

    while (iter.hasNext()) {
      CacheUnit unit = iter.next();
      PreviousIterator<CacheInternalUnit> prevIter = tmpUnit.previousIterator();

      CacheUnit tmpunit = ClientCacheContext.UTILS.getKeyByReverse(unit.getBegin(), unit.getEnd(),
        unit.getFileId(), prevIter, -1, new UnlockTask());
      if (!tmpunit.isFinish()) {
        ClientCacheContext.UTILS.convertCache((TempCacheUnit) tmpunit, tmpUnit);
      }
    }
    Iterator<CacheInternalUnit> tmpIter = tmpUnit.iterator();
    while (tmpIter.hasNext()) {
      CacheInternalUnit i = tmpIter.next();
      res += (i.getEnd() - i.getBegin());
    }
    return res;
  }

  @Override
  public double function(CacheUnit e) {
    return e.getEnd() - e.getBegin();
  }

  @Override
  public double upperBound(Set<CacheUnit> input, Set<CacheUnit> baseSet) {

    CacheSet prevElements = (CacheSet) mSetUtils.subtract(baseSet, input);
    CacheSet newElements = (CacheSet) mSetUtils.subtract(input, baseSet);
    double needDelete = 0;
    if (!prevElements.isEmpty()) {
      for (Map.Entry entry : prevElements.cacheMap.entrySet()) {
        for (CacheUnit j : (Set<CacheUnit>) entry.getValue()) {
          Set<CacheUnit> left = mSetUtils.subtract(baseSet, j);
          needDelete += (function(baseSet) - function(left));
        }
      }
    }
    //System.out.println(needDelete);
    double needAdd = 0;
    //double emptyValue = 0;
    if (!newElements.isEmpty()) {
      for (Map.Entry entry : newElements.cacheMap.entrySet()) {
        for (CacheUnit unit : (Set<CacheUnit>) entry.getValue()) {
          needAdd += (unit.getEnd() - unit.getBegin());
        }
      }
    }
    // System.out.println("base " + function(baseSet));
    // System.out.println("delete " + needDelete);
    // System.out.println("add " + needAdd);

    return function(baseSet) - needDelete + needAdd;
  }
}
