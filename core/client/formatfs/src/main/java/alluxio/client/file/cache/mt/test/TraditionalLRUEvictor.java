package alluxio.client.file.cache.mt.test;

import alluxio.client.file.cache.core.BaseCacheUnit;
import org.apache.commons.lang3.RandomUtils;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class TraditionalLRUEvictor extends TraditionalLFUEvictor {
  private Set<pair> s = new HashSet<>();
  private LinkedList<pair> visitMap = new LinkedList<>();
  public long limit = 1024 * 1024 * 1000;

  public TraditionalLRUEvictor() {
  }


  public PolicyName getPolicyName() {
    return PolicyName.TRADITIONAL_LRU;
  }

  public long evict() {
    long size = visitMap.size() * blockSize;
    long deleteSize = 0;
    while (size > limit) {
      //System.out.println(size / (1024  *1024));
      pair p = visitMap.pollFirst();
      s.remove(p );
      removeBlockFromCachaSpace(p.fileId, p.index);

      deleteSize += blockSize;
      size = visitMap.size() * blockSize;
    }
    return deleteSize;
  }

  public void add(BaseCacheUnit unit) {
    List<Integer> tmp = getInvolvedBlock(unit.getBegin(), unit.getEnd());

    for (int i : tmp) {
      pair p = new pair(unit.mIndex, i);
      if (s.contains(p)) {
        visitMap.remove(p);
        visitMap.addLast(p);
      } else {
        visitMap.addLast(p);
        s.add(p);
        addIntoCacheSpace(p.fileId, p.index);
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
      long fileId = unit.getFileId();
      boolean rightLarge, leftSmall;
      if (tmp.size() == 0) {
        rightLarge = leftSmall= false;
      } else {
        rightLarge = unit.getEnd() > (tmp.get(tmp.size() - 1) + 1) * blockSize;
        leftSmall = unit.getBegin() < tmp.get(0) * blockSize + 1;
      }
      for (int i : tmp) {
        if (visitMap.contains(new pair(fileId, i))) {
          visitSize += blockSize;
        }
      }
      if (leftSmall && visitMap.contains(new pair(fileId, tmp.get(0) - 1))) {
        visitSize += (tmp.get(0) ) * blockSize - unit.getBegin();
        // System.out.println("left : " + (double)((tmp.get(0) ) * blockSize - unit.getBegin()) / (double) (1024 * 1024));
      }
      if (rightLarge && visitMap.contains(new pair(fileId, tmp.get(tmp.size() - 1) + 1))) {
        visitSize += unit.getEnd() -( tmp.get(tmp.size() - 1)  +1 )* blockSize;
        //System.out.println("right " + (double)(unit.getEnd() -( tmp.get(tmp.size() - 1)  +1 )* blockSize) / (double) (1024 * 1024));

      }
      allVisitSize += unit.getSize();

    }
    System.out.println("hitRatio by size : " + ((double) visitSize / (double) allVisitSize));

  }
  public static void main(String[] args) throws Exception{
    TraditionalLRUEvictor test = new TraditionalLRUEvictor();
    test.init(1024 * 1024 * 600);
    test.testVisit();
  }

  class pair {
    long fileId;
    int index;

    public pair(long id, int index) {
      fileId = id;
      this.index = index;
    }

    @Override
    public int hashCode() {
      return (int)(fileId * 31 + index) * 31;
    }

    @Override
    public boolean equals(Object obj) {
      if (! (obj instanceof pair)) {
        return false;
      }
      pair p = (pair)obj;
      return fileId == p.fileId && index == p.index;
    }
  }



}
