package alluxio.client.file.cache.test.mt;

import alluxio.client.file.cache.ClientCacheContext;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

public class ESFEvictor extends MTLRUEvictor {

  private long mBase = 1024 * 1024;

  public ESFEvictor(ClientCacheContext context) {
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
      LFUEvictContext base = new LFUEvictContext(this, new ClientCacheContext(false), userId);
      base.resetCapacity(cacheSize);
      baseEvictCotext.put(userId, base);
    }
    baseEvictCotext.get(userId).accessByShare(unit, mContext);
    baseEvictCotext.get(userId).evict();
  }

  private double getHRDCost(double HRD, long userId) {
    //System.out.println(HRD  + " : " + userId + " : " +( ((double)(actualEvictContext.get(userId).mCacheSize) / (double)cacheSize)));
    double usedRatio = ((actualEvictContext.get(userId).mCacheSize) / cacheSize);
    //System.out.println("user " + userId + " " + HRD + " : " + usedRatio);

    return (HRD) * 100 * usedRatio;
  }

  private double getHRDCostWhenCheat(double HRD, long userId) {
    double usedRatio = ((double)(actualEvictContext.get(userId).mCacheSize) / (double)cacheSize);
    if(usedRatio == 0) {
      return 0;
    }
    return (HRD) * 100 / usedRatio;
  }

  private boolean isIsolateGanaratee(long userId) {
    double usedRatio = ((double)(actualEvictContext.get(userId).mCacheSize) / (double)cacheSize);
    double hitRatio =(actualEvictContext.get(userId).computePartialHitRatio() / baseEvictCotext.get(userId).computePartialHitRatio());
    double lowestRatio =  1 / (double)actualEvictContext.size();
    //System.out.println("test " + usedRatio + " : " + hitRatio + " : " + lowestRatio);
    return usedRatio >= lowestRatio && hitRatio >= lowestRatio;
  }

  private long getMostUsedCache() {
    double maxSize = 0;
    long maxUserId = -1;
    for (long userId : actualEvictContext.keySet()) {
      BaseEvictContext context = actualEvictContext.get(userId);
      double usedSize = context.mCacheSize;
      if (usedSize > maxSize) {
        maxSize = usedSize;
        maxUserId = userId;
      }
    }
    return maxUserId;
  }

  private long getLargestHRD() {
    double minHRDCost = Integer.MAX_VALUE;
    long minCostUserId = -1;
    for (long userId : actualEvictContext.keySet()) {
      double actualHitRatio = actualEvictContext.get(userId).computePartialHitRatio();
      double baseHitRatio = baseEvictCotext.get(userId).computePartialHitRatio();
      double HRDCost = getHRDCostWhenCheat(baseHitRatio - actualHitRatio, userId);
      System.out.println(baseHitRatio + " " +  actualHitRatio + "  " +((double)(actualEvictContext.get(userId).mCacheSize) / (double)cacheSize));
      if (HRDCost < minHRDCost && actualEvictContext.get(userId).mCacheSize > 0
              && actualEvictContext.get(userId).getEvictUnit()!= null){
        minHRDCost = HRDCost;
        minCostUserId = userId;
      }
    }
    return minCostUserId;
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
        //System.out.println(userId  +" " + actualHitRatio + " "+baseHitRatio + " " +HRDCost);
       // System.out.println(userId + " cache size " +(actualEvictContext.get(userId).mCacheSize) / (1024 * 1024) + " || " + cacheSize/ ( 1024 * 1024));
        if (HRDCost < minHRDCost && actualEvictContext.get(userId).mCacheSize > 0
                && actualEvictContext.get(userId).getEvictUnit()!= null
                ){
          minHRDCost = HRDCost;
          minCostUserId = userId;
        }
      }
      if (minCostUserId == -1) {
        //System.out.println("===========================");
        minCostUserId = getLargestHRD();
       // System.out.println(minCostUserId);
       // System.out.println("===========================");
      }

      //System.out.println("evict " + minCostUserId);

      TmpCacheUnit unit = actualEvictContext.get(minCostUserId).getEvictUnit();
      actualSize -=  actualEvictContext.get(minCostUserId).remove(unit);
      checkRemoveByShare(unit, minCostUserId);
    }
  }

  public static void main(String [] args) {
    ESFEvictor esfTest = new ESFEvictor(new ClientCacheContext(false));
    esfTest.test();
  }
}
