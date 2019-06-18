package alluxio.client.file.cache.mt.run;

import alluxio.client.file.cache.core.ClientCacheContext;

public class PFEvictor extends MTLRUEvictor {

  public PFEvictor(ClientCacheContext context) {
    super(context);
  }

  @Override
  public void access(long userId, TmpCacheUnit unit) {
    if (!actualEvictContext.containsKey(userId)) {
      actualEvictContext.put(userId, new LFUEvictContext(this, mContext, userId));
    }
    long actualNew = actualEvictContext.get(userId).access(unit);

    mAccessSize += unit.getSize();
    mHitSize += unit.getSize() - actualNew;
    actualSize += actualNew;

    if (actualSize > cacheSize) {
      evict();
    }

    if (!baseEvictCotext.containsKey(userId)) {
      LRUEvictContext base = new LRUEvictContext(this, new ClientCacheContext(false), userId);
      base.resetCapacity(cacheSize);
      baseEvictCotext.put(userId, base);
    }
    baseEvictCotext.get(userId).accessByShare(unit, mContext);
    baseEvictCotext.get(userId).evict();
  }

  private double computePFValue(long userId) {
    LFUEvictContext context = (LFUEvictContext)actualEvictContext.get(userId);
    TmpCacheUnit needDelete = context.getEvictUnit();
    if (needDelete == null) {
      return 0 ;
    }
    double res = 0;
    for(long tmpId : actualEvictContext.keySet()) {
      LFUEvictContext context1 = (LFUEvictContext)actualEvictContext.get(tmpId);
      double tmpSum = 0;
      System.out.println(context1.mAccessMap.size());
      for (TmpCacheUnit tmp : context1.mAccessMap.keySet()) {
        if (!tmp.equals(needDelete)) {
          tmpSum += context1.mAccessMap.get(tmp);
        }
      }
      res += Math.log(tmpSum);
      //res += tmpSum;
    }
    double usedRatio =  actualEvictContext.get(userId).mCacheSize / (double)cacheSize;
    System.out.println(res + " " + usedRatio);
    return res / (usedRatio == 0? 0.0001 : usedRatio);
  }

  public double  computePFValueWhenCheat(long userId) {
    LFUEvictContext context = (LFUEvictContext)actualEvictContext.get(userId);
    TmpCacheUnit needDelete = context.getEvictUnit();
    if (needDelete == null) {
      return Integer.MIN_VALUE ;
    }
    double res = 0;
    for(long tmpId : actualEvictContext.keySet()) {
      LFUEvictContext context1 = (LFUEvictContext)actualEvictContext.get(tmpId);
      double tmpSum = 0;
      for (TmpCacheUnit tmp : context1.mAccessMap.keySet()) {
        if (!tmp.equals(needDelete)) {
          tmpSum += context1.mAccessMap.get(tmp);
        }
      }
       res += Math.log(tmpSum);
      //res += tmpSum;
    }
    return res;
  }

  public void evict() {
    while (actualSize > cacheSize) {
      double maxValue = Integer.MIN_VALUE;
      long maxCostId = -1;
      System.out.println("============");
      for (long userId : actualEvictContext.keySet()) {
        double tmpCost =  computePFValueWhenCheat(userId);
        if (tmpCost >= maxValue ) {
          maxValue = tmpCost;
          maxCostId = userId;

        }
        System.out.println("============ " + userId + " " + tmpCost);

      }
      System.out.println(maxCostId);
      //System.out.println(maxCostId +  " " + maxValue);
      TmpCacheUnit unit = actualEvictContext.get(maxCostId).getEvictUnit();
      actualSize -=  actualEvictContext.get(maxCostId).remove(unit);
      checkRemoveByShare(unit, maxCostId);
    }
  }

  public static void main(String[] args) {
    PFEvictor test = new PFEvictor(new ClientCacheContext(false));
    test.testUserNum_5();
   // ExcelTest.generateFile();

  }
}
