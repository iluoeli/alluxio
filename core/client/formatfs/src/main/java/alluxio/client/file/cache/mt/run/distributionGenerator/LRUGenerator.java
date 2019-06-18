package alluxio.client.file.cache.mt.run.distributionGenerator;

import org.apache.commons.lang3.RandomUtils;

import java.util.*;

public class LRUGenerator implements Generator {
  private int base;
  private Set<Integer> mAccessList = new HashSet<>();
  private List<Integer> ll = new ArrayList<>();
  private int LRUBase = 3;

  public LRUGenerator(int R) {
    base = R;
  }
  //size为rank个数，skew为数据倾斜程度, 取值为0表示数据无倾斜，取值越大倾斜程度越高


  public int next() {         // [1,n]
    int res = RandomUtils.nextInt(0, base);
    int isVisitBefore = RandomUtils.nextInt(0, LRUBase);
    if (isVisitBefore == 0 && mAccessList.size() > base / LRUBase) {
      int res1 = RandomUtils.nextInt(0, mAccessList.size());
      return ll.get(res1);
    } else {
      if (!mAccessList.contains(res)) {
        mAccessList.add(res);
        ll.add(res);
      }
      return res;
    }

  }
}
