package file.cache.test;

import file.cache.BaseCacheUnit;
import file.cache.ClientCacheContext;
import org.apache.commons.lang3.RandomUtils;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class OriginLRUEvictor extends LFUEvictor {
  private Set<Integer> s = new HashSet<>();
  private LinkedList<Integer> visitMap = new LinkedList<>();
  public long limit = 1024 * 1024 * 1000;

  public OriginLRUEvictor(ClientCacheContext context) {
    super(context);
  }


  public void evict() {
    long size = visitMap.size() * blockSize;
    while (size > limit) {
      //System.out.println(size / (1024  *1024));

      s.remove( visitMap.pollFirst());
      size = visitMap.size() * blockSize;
    }
    System.out.println(size / (1024  *1024));
  }

  public void add(BaseCacheUnit unit) {
    List<Integer> tmp = getInvolvedBlock(unit.getBegin(), unit.getEnd());

    for (int i : tmp) {
      if (s.contains(i)) {
        visitMap.remove(new Integer(i));
        visitMap.addLast(i);
      } else {
        visitMap.addLast(i);
        s.add(i);
      }
    }
  }

  public void init(long limit) throws Exception {
    long sum = 0;
    for(int i = 0 ; i < 1200; i ++) {
      long length = RandomUtils.nextLong(1024 * 1024, 1024 * 1024 * 4);
      long begin = RandomUtils.nextLong(0, 1024 * 1024 * 1024 - length);
      sum += length;
      BaseCacheUnit unit = new BaseCacheUnit(mTestFileId, begin, begin + length);
      visitList.add(unit);
      add(unit);
    }
    evict();
    System.out.println(visitMap.size() * blockSize / (1024 *1024));
  }

  public void testVisit() {
    double visitTime = 0;
    long visitSize = 0;
    long allVisitSize = 0;
    for (BaseCacheUnit unit : visitList) {
      List<Integer> tmp = getInvolvedBlock(unit.getBegin(), unit.getEnd());

      boolean rightLarge, leftSmall;
      if (tmp.size() == 0) {
        rightLarge = leftSmall= false;
      } else {
        rightLarge = unit.getEnd() > (tmp.get(tmp.size() - 1) + 1) * blockSize;
        leftSmall = unit.getBegin() < tmp.get(0) * blockSize + 1;
      }
      for (int i : tmp) {
        if (visitMap.contains(i)) {
          visitSize += blockSize;
        }
      }
      if (leftSmall && visitMap.contains(tmp.get(0) - 1)) {
        visitSize += (tmp.get(0) ) * blockSize - unit.getBegin();
        // System.out.println("left : " + (double)((tmp.get(0) ) * blockSize - unit.getBegin()) / (double) (1024 * 1024));
      }
      if (rightLarge && visitMap.contains(tmp.get(tmp.size() - 1) + 1)) {
        visitSize += unit.getEnd() -( tmp.get(tmp.size() - 1)  +1 )* blockSize;
        //System.out.println("right " + (double)(unit.getEnd() -( tmp.get(tmp.size() - 1)  +1 )* blockSize) / (double) (1024 * 1024));

      }
      allVisitSize += unit.getSize();

    }
    System.out.println("hitRatio by size : " + ((double) visitSize / (double) allVisitSize));

  }
  public static void main(String[] args) throws Exception{
    OriginLRUEvictor test = new OriginLRUEvictor(new ClientCacheContext(false));
    test.init(1024 * 1024 * 600);
    test.testVisit();
  }
}
