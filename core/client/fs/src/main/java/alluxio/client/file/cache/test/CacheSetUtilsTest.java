package alluxio.client.file.cache.test;

import alluxio.client.file.cache.CacheInternalUnit;
import alluxio.client.file.cache.CacheUnit;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSetUtils;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSpaceCalculator;

import java.util.Map;
import java.util.Set;

public class CacheSetUtilsTest {
  CacheSet input = new CacheSet();
  CacheSet input2 = new CacheSet();
  CacheSetUtils util = new CacheSetUtils();
  public void init() {
    input.clear();
    //for (int i = 0 ; i < 1000 ; i ++) {
   //   input.add(new CacheInternalUnit(i, i+1, 1));
     // input.add(new CacheInternalUnit(9, 21, 1));
      //input.add(new CacheInternalUnit(20, 30, 1));
     // input.add(new CacheInternalUnit(29, 40, 1));
   // }

    input.add(new CacheInternalUnit(0,10,2));
    input.add(new CacheInternalUnit(9,20,2));
    input.add(new CacheInternalUnit(40,50,2));
  }

  public static void print(CacheSet set) {
    for (Map.Entry entry : set.cacheMap.entrySet()) {
      for(CacheUnit unit : (Set<CacheUnit>)entry.getValue()) {
        System.out.println(unit.getBegin() + " " + unit.getEnd());
      }
    }
  }
  public void subtractTest() {
    init();

    print((CacheSet)util.subtract(input, new CacheInternalUnit(0,10,1)));
    System.out.println(input);
  }

  public void iteratorTest() {
    init();
    long begin = System.currentTimeMillis();
    long tmp = 0 ;
    for(int i = 1; i < 1000000 ; i ++) {
      if(input.contains(new CacheInternalUnit(i,i +1,1))) {
        tmp ++;
      }
    }
    System.out.println(System.currentTimeMillis() - begin);
  }

  public void spaceTest() {
  	init();
		System.out.println(new CacheSpaceCalculator().function(input));
	}

  public static void main(String [] args) {

    new CacheSetUtilsTest().spaceTest();
   // CacheSet
  }
}
