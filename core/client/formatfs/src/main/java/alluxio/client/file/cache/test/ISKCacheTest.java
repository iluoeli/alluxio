package alluxio.client.file.cache.test;

import alluxio.client.file.cache.*;
import alluxio.client.file.cache.struct.DoubleLinkedList;
import alluxio.client.file.cache.struct.LongPair;
import alluxio.client.file.cache.submodularLib.ISK;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSpaceCalculator;
import alluxio.exception.AlluxioException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.RandomUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

public class ISKCacheTest {
  CacheSet input = new CacheSet();
  ISK isk = new ISK(1024 * 1024 * 512, null);
  private long fileLength = 1024 * 1024 * 1024;
  private long fileId = 1;
  private List<BaseCacheUnit> visitList = new ArrayList<>();
  private ClientCacheContext context = new ClientCacheContext(false);

  public void test1() throws Exception {
    PromotionPolicy promoter = new PromotionPolicy();
    promoter.init(1024 * 1024 * 500);
    promoter.addFileLength(fileId, fileLength);
    CacheSet s = new CacheSet();
    long sum = 0L;
    for(int i = 0 ; i < 1200; i ++) {
       long length = RandomUtils.nextLong(1024 * 1024, 1024 * 1024 * 4);
       long begin = RandomUtils.nextLong(0, 1024 * 1024 * 1024 - length);
       sum += length;
       BaseCacheUnit unit = new BaseCacheUnit(fileId, begin, begin + length);
       promoter.filter(unit);
       visitList.add(unit);
       s.add(unit);
       s.addSort(unit);
    }
    System.out.println("all " + sum /(1024 * 1024));
    System.out.println("overlap delete : " + context.getAllSize(s, context)/ (1024 * 1024));

    promoter.updateTest(context);
  }

  public void testVisit() {
    double visitTime = 0;
    long visitSize = 0;
    long allVisitSize = 0;
    for (BaseCacheUnit unit : visitList) {
      allVisitSize += unit.getSize();
      CacheUnit unit1 = context.getCache(fileId, fileLength, unit.getBegin(), unit.getEnd(), new UnlockTask());
      if (unit1.isFinish()) {
        visitSize += unit.getSize();
        visitTime ++;
      } else {
        TempCacheUnit unit2 = (TempCacheUnit)unit1;
        long m = 0;
        for (CacheInternalUnit unit3 : unit2.mCacheConsumer) {
          m += unit3.getSize();
        }
        long missSize = unit2.getSize() - m;
        long hitSize = unit.getSize() - missSize;
        visitSize += hitSize;
        visitTime += ((double)hitSize /(double) unit.getSize());
      }
    }
    System.out.println("hitRatio by size : " +( (double)visitSize / (double)allVisitSize ));
    System.out.println("hitRatio by time : " + (visitTime / (double)visitList.size()));
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
  	ISKCacheTest test = new ISKCacheTest();
  	test.test1();
  	test.testVisit();
	}
}
