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
      actualEvictContext.put(userId, new LRUEvictContext(this, mContext));
    }
    long actualNew = actualEvictContext.get(userId).access(unit);

    mAccessSize += unit.getSize();
    mHitSize += unit.getSize() - actualNew;
    actualSize += actualNew;

    if (!baseEvictCotext.containsKey(userId)) {
      LRUEvictContext base = new LRUEvictContext(this, new ClientCacheContext(false));
      base.resetCapacity((long)(cacheSize));
      baseEvictCotext.put(userId, base);
    }
    baseEvictCotext.get(userId).accessByShare(unit, mContext);
    baseEvictCotext.get(userId).evict();
    if (actualSize > cacheSize) {
      evict();
    }
  }

  private double getHRDCost(double HRD, long userId) {
    return (HRD) * 100 /( (double)actualEvictContext.get(userId).mCacheSize /  mBase );
  }

  public void evict() {
    while (actualSize > cacheSize) {
      double minHRDCost = Integer.MAX_VALUE;
      long minCostUserId = -1;
      for (long userId : actualEvictContext.keySet()) {
        double actualHitRatio = actualEvictContext.get(userId).computePartialHitRatio();
        double baseHitRatio = baseEvictCotext.get(userId).computePartialHitRatio();
        double HRDCost = getHRDCost(baseHitRatio - actualHitRatio, userId);
        if (HRDCost < minHRDCost) {
          minHRDCost = HRDCost;
          minCostUserId = userId;
        }
      }
      TmpCacheUnit unit = actualEvictContext.get(minCostUserId).getEvictUnit();
      actualSize -=  actualEvictContext.get(minCostUserId).remove(unit);
    }
  }

  public static void main(String [] args) {
    ESFEvictor esfTest = new ESFEvictor(new ClientCacheContext(false));
    esfTest.test();
  }

}
