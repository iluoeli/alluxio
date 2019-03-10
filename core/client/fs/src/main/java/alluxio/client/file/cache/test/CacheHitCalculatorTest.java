package alluxio.client.file.cache.test;

import alluxio.client.file.cache.CacheInternalUnit;
import alluxio.client.file.cache.CacheUnit;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheHitCalculator;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSetUtils;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSpaceCalculator;

import java.util.HashMap;

public class CacheHitCalculatorTest {
  CacheHitCalculator cacheHitCalculator = new CacheHitCalculator(new CacheSetUtils());
  CacheSpaceCalculator calculator = new CacheSpaceCalculator();

  CacheSet input = new CacheSet();

  public void init() {
    input.clear();
    input.add(new CacheInternalUnit(0,10,1));
    input.add(new CacheInternalUnit(5,20,1));
    input.add(new CacheInternalUnit(15,30,1));
    input.add(new CacheInternalUnit(25,40,1));
  }

  public void testCopy() {
    HashMap<Long, HashMap<CacheUnit, Double>> res = new HashMap<>();
    HashMap<CacheUnit, Double> m = new HashMap<>();
    m.put(new CacheInternalUnit(1,2,1),0d);
    m.put(new CacheInternalUnit(3,4,1), 3d);
    res.put(1l, m);
    HashMap<Long, HashMap<CacheUnit, Double>> t = cacheHitCalculator.copy(res);
    res.get(1l).put(new CacheInternalUnit(1,2,1), 2d);
    System.out.println(t.toString());
    System.out.println(res.toString());
  }

  public void testFunction() {
    init();
		input.sortConvert();
		cacheHitCalculator.function(input);
    System.out.println(cacheHitCalculator.mHitRatioMap.toString());
    }


  public void FunctionTest() {
    init();
    input.sortConvert();
    double res = calculator.function(input);
    System.out.println(res);
  }

  public void upperBoundTest() {
    init();
    CacheSet input2 = new CacheSet();
    input2.add(new CacheInternalUnit(0,10,1));
    input2.add(new CacheInternalUnit(0,10,2));
    input2.add(new CacheInternalUnit(5,20,2));

    double res = calculator.upperBound(input2, input);
    System.out.println(res);

  }

  public static void main(String [] args) {
    new CacheHitCalculatorTest().testFunction();
  }
}
