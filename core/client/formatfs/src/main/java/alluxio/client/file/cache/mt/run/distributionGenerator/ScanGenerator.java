package alluxio.client.file.cache.mt.run.distributionGenerator;

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
