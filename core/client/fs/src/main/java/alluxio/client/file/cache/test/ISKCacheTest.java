package alluxio.client.file.cache.test;

import alluxio.client.file.cache.BaseCacheUnit;
import alluxio.client.file.cache.CacheInternalUnit;
import alluxio.client.file.cache.LinkedFileBucket;
import alluxio.client.file.cache.struct.DoubleLinkedList;
import alluxio.client.file.cache.submodularLib.ISK;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSpaceCalculator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.File;
import java.io.FileOutputStream;
import java.util.*;

public class ISKCacheTest {
  CacheSet input = new CacheSet();
  ISK isk = new ISK(2000, null);
  public void init() {
    input.clear();
    //input.add(new CacheInternalUnit(0,15,1));
    //input.add(new CacheInternalUnit(10,25,1));
    //input.add(new CacheInternalUnit(20,35,1));
    //input.add(new CacheInternalUnit(30,45,1));

    for(int i = 0 ; i < 2000; i ++) {
       Random r = new Random();
       int  begin = r.nextInt(1000);
       int length = r.nextInt(1000);
       int fileId = r.nextInt(1);
      input.add(new BaseCacheUnit(begin, begin+length, fileId));

      input.addSort(new BaseCacheUnit(begin, begin+length, fileId));
    }

  }

  public void test() {
    init();
    isk.addInputSpace(input);
		System.out.println(new CacheSpaceCalculator().function(input));
    long begin = System.currentTimeMillis();
		isk.optimize();
    CacheSet res = isk.getResult();
    System.out.println(System.currentTimeMillis() - begin);

    System.out.println(new CacheSpaceCalculator().function(res));
    System.out.println("end");
    System.out.println(isk.iterNum);
  }

  public static void Test(List<Integer> e, List<List<Integer>> rew) {
    rew.add(e);
  }

  public static void test(CacheInternalUnit i) {
    i = null;
  }

  public static void testList() {
    long begin = System.currentTimeMillis();
    DoubleLinkedList<CacheInternalUnit> l1 = new DoubleLinkedList<>(new CacheInternalUnit(0, 0,-1));
    LinkedList<CacheInternalUnit> l2 = new LinkedList<>();
    for (int i = 0 ; i < 1000000 ; i ++) {
      CacheInternalUnit u = new CacheInternalUnit(1, i, 1);
      l2.add(u);
    }
    System.out.println(System.currentTimeMillis() - begin);
  }

  public static void testMap() {
    HashMap<Integer, LinkedFileBucket> map = new HashMap<>();
    for(int i = 0 ; i < 1000000; i++) {
      int j = i % 10;
      LinkedFileBucket l = map.get(j);
      if(l == null) {
        map.put(j, new LinkedFileBucket(100000,1, null));
      }
    }
  }

  public static void testArray() {
    LinkedFileBucket[] l = new LinkedFileBucket[10];
    for(int j = 0 ; j <1000000; j++) {
      int i = j % 10;
      if(l[i] == null) {
        l[i] = new LinkedFileBucket(100000,1, null);
      }
    }
  }

  public static void main(String [] args) throws Exception{
  	new ISKCacheTest().test();
	}
}
