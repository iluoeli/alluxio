package alluxio.client.file.cache.test.MTTest;

import alluxio.client.file.cache.ClientCacheContext;

public class ESFEvictor extends MTLRUEvictor {

  private long mBase = 1024 * 1024;

  public ESFEvictor(ClientCacheContext context) {
    super(context);
  }

  @Override
  public void access(long userId, TmpCacheUnit unit) {
    if (!actualEvictContext.containsKey(userId)) {
      actualEvictContext.put(userId, new LRUEvictContext(this, mContext, userId));
    }
    long actualNew = actualEvictContext.get(userId).access(unit);

    mAccessSize += unit.getSize();
    mHitSize += unit.getSize() - actualNew;
    actualSize += actualNew;

    if (!baseEvictCotext.containsKey(userId)) {
      LRUEvictContext base = new LRUEvictContext(this, new ClientCacheContext(false), userId);
      base.resetCapacity(cacheSize);
      baseEvictCotext.put(userId, base);
    }
    baseEvictCotext.get(userId).accessByShare(unit, mContext);
    baseEvictCotext.get(userId).evict();
    if (actualSize > cacheSize) {
      evict();
    }

    mBaseEvictContext.access(unit);
    mBaseEvictContext.evict();
  }

  private double getHRDCost(double HRD, long userId) {
    return (HRD) * 100 * ((actualEvictContext.get(userId).mCacheSize) / cacheSize);
  }

  public void evict() {
    //System.out.println("----------------");
    while (actualSize > cacheSize) {
      double minHRDCost = Integer.MAX_VALUE;
      long minCostUserId = -1;
      for (long userId : actualEvictContext.keySet()) {
        double actualHitRatio = actualEvictContext.get(userId).computePartialHitRatio();
        double baseHitRatio = baseEvictCotext.get(userId).computePartialHitRatio();
        double HRDCost = getHRDCost(baseHitRatio - actualHitRatio, userId);
      //  System.out.println(userId  +" " + actualHitRatio + " "+baseHitRatio + " " +HRDCost);
        if (HRDCost < minHRDCost && actualEvictContext.get(userId).mCacheSize > 0 ){
          minHRDCost = HRDCost;
          minCostUserId = userId;
        }
      }

      //System.out.println(minCostUserId);

      TmpCacheUnit unit = actualEvictContext.get(minCostUserId).getEvictUnit();
      actualSize -=  actualEvictContext.get(minCostUserId).remove(unit);
      checkRemoveByShare(unit, minCostUserId);
    }
   // System.out.println("================");
  }

  public static void main(String [] args) {
    ESFEvictor esfTest = new ESFEvictor(new ClientCacheContext(false));
    esfTest.test();
  }
}
