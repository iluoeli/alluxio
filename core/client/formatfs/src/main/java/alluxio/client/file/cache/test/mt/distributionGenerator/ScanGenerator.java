package alluxio.client.file.cache.test.mt.distributionGenerator;

import org.apache.commons.lang3.RandomUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ScanGenerator  implements Generator  {
  private int base;
  private int lastNum;

  public ScanGenerator(int R) {
    base = R;
    lastNum =0;
  }


  public int next() {
    if (lastNum == base) {
      lastNum = 0;
    }

    return lastNum ++ ;
  }
}
