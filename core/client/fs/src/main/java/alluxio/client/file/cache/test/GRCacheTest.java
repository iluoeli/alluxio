package alluxio.client.file.cache.test;

import alluxio.client.file.cache.BaseCacheUnit;
import alluxio.client.file.cache.CacheInternalUnit;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSpaceCalculator;
import alluxio.client.file.cache.submodularLib.cacheSet.DivideGR;
import alluxio.client.file.cache.submodularLib.cacheSet.GR;

public class GRCacheTest {
  GR gr = new GR(10000 , null);
  DivideGR divideGR = new DivideGR();
  CacheSet input = new CacheSet();

  public void init() {
    input.clear();
    //input.add(new CacheInternalUnit(0,15,1));
    //input.add(new CacheInternalUnit(10,25,1));
    //input.add(new CacheInternalUnit(20,35,1));
    //input.add(new CacheInternalUnit(30,45,1));

    for(int i = 0 ; i < 100; i ++) {
     // Random r = new Random();
     // int begin = r.nextInt(1000);
     // int length = r.nextInt(1000);
     // int fileId = r.nextInt(3);
     // input.add(new BaseCacheUnit(begin, begin+length, fileId));
      input.add(new BaseCacheUnit(1, i*1000, i * 1000 + 1000));
    }
  }

  public void divideTest() {
    CacheInternalUnit unit = new CacheInternalUnit(1,100,1);
    BaseCacheUnit t1 = new BaseCacheUnit(1,10,1);
    BaseCacheUnit t2 = new BaseCacheUnit(5,25,1);
    BaseCacheUnit t3 = new BaseCacheUnit(40, 50,1);
    BaseCacheUnit t4 = new BaseCacheUnit(45, 70,1);
    divideGR.fliter(unit, t1);
    divideGR.fliter(unit, t2);
    divideGR.fliter(unit, t3);
    divideGR.fliter(unit, t4);
    while(!divideGR.hitRatioQueue.isEmpty()) {
      BaseCacheUnit unit1 = divideGR.hitRatioQueue.poll();
      System.out.println(unit1 + " " + unit1.getHitValue());
    }
    divideGR.deleteCache(unit);
  }

  public void grTest() {
    long begin = System.currentTimeMillis();
    init();
    input.convertSort();
    gr.addInputSpace(input);
    System.out.println(new CacheSpaceCalculator().function(input));
    gr.optimize();
    CacheSet res = (CacheSet) gr.getResult();
    System.out.println(new CacheSpaceCalculator().function(res));
    System.out.println("test : " + (System.currentTimeMillis() - begin));
  }

  public static void main(String[] args) {
    GRCacheTest gr = new GRCacheTest();
    gr.grTest();


  }
}
