package alluxio.client.file.cache.test.mt;

import alluxio.client.file.cache.ClientCacheContext;
import org.apache.commons.lang3.RandomUtils;

public class FairrideEvictor extends MMFEvictor {

  public FairrideEvictor(ClientCacheContext context) {
    super(context);
  }

  @Override
  public void access(long userId, TmpCacheUnit unit) {
    if (mShareSet.containsKey(unit)) {
      if(!(mShareSet.get(unit).contains(userId) && mShareSet.size() == 1)) {
        int shareSize = mShareSet.get(unit).size();
        double random = 1 / (double) shareSize;
        double pro = RandomUtils.nextDouble(0, 1);
        if (pro > random) {
         // System.out.println("Test =================");
          return;
        }
      }
    }
    super.access(userId, unit);
  }

  @Override
  public void cheatAccess(TmpCacheUnit unit, long userId) {
    //System.out.println("cheat access =============== ");

    //return;

    if (mShareSet.containsKey(unit)) {
      //if(!( mShareSet.size() == 1)) {
        int shareSize = mShareSet.get(unit).size() + 1;
        double random = 1 / (double) shareSize;
        double pro = RandomUtils.nextDouble(0, 1);
        if (pro < random) {
          System.out.println("cheat access =====----------= ");

          return;
        }
     // }
    }
    long actualNew = actualEvictContext.get(userId).access(unit);
    if (actualNew > 0) {
      actualSize += actualNew;
      while (actualSize > cacheSize) {
        evict();
      }
    }
  }


  public static void main(String[] args) {
    FairrideEvictor test = new FairrideEvictor(new ClientCacheContext(false));
    test.testCheatAccess();
    ExcelTest.generateFile();

  }
}
