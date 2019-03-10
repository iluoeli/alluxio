package alluxio.client;

import alluxio.collections.ConcurrentHashSet;
import sun.misc.Cache;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class listTest {

  public static void main(String [] args) {

      TreeSet<CacheUnit> ss = new TreeSet<>();
        Queue<CacheUnit> q = new LinkedList<>();

     for (int i = 0 ;i < 1000000; i ++) {
        long begin = (long)(Math.random() * 10000000);
        long end = begin + 1000;
        CacheUnit u = new CacheUnit(begin, end, new byte[1000]);
       ss .add(u);
       // System.out.println(i);
        q.add(u);
      }
    long begin1 = System.currentTimeMillis();

    for (int i = 0 ;i < 1000000; i ++) {
      ss.contains(q.poll());
      //System.out.println(i);
    }
    System.out.println(System.currentTimeMillis() - begin1);

  }
  public static class CacheUnit implements Comparable {
    public long begin;
    public long end;
    public byte[] b ;

    public CacheUnit(long begin, long end , byte[] bb) {
      b = bb;
      this.begin = begin;
      this.end = end;
    }

    @Override
    public int hashCode() {
      return (int)begin % 2;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof  CacheUnit) {
        return begin == ((CacheUnit)obj).begin && end == ((CacheUnit)obj).end;
      }
      return false;
    }

    @Override
    public int compareTo(Object o) {
      return (int) (begin - ((CacheUnit)o).begin);
    }
  }
}
