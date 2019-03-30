package alluxio.client.file.cache.test.mt;

import alluxio.client.file.cache.ClientCacheContext;

public class MMFEvictor extends MTLRUEvictor {

  public MMFEvictor(ClientCacheContext context) {
    super(context);
  }


  @Override
  public void access(long userId, TmpCacheUnit unit) {
    if (!actualEvictContext.containsKey(userId)) {
      //if (userId == 1) {
        actualEvictContext.put(userId, new LRUEvictContext(this, mContext, userId));
     // } else if (userId == 2) {
     //   actualEvictContext.put(userId, new LFUEvictContext(this, mContext, userId));
     // } else {
     //   actualEvictContext.put(userId, new LIRSEvictContext(this, mContext, userId, 330));
     // }
    }
    long actualNew = actualEvictContext.get(userId).access(unit);
    mAccessSize += unit.getSize();
    mHitSize += unit.getSize() - actualNew;
    if (actualNew > 0) {
      actualSize += actualNew;
      if (actualSize > cacheSize) {
        evict();
      }
    }

    if (!baseEvictCotext.containsKey(userId)) {
      LRUEvictContext base = new LRUEvictContext(this, new ClientCacheContext(false), userId);
      base.resetCapacity(cacheSize);
      baseEvictCotext.put(userId, base);
    }
    baseEvictCotext.get(userId).accessByShare(unit, mContext);
    baseEvictCotext.get(userId).evict();

  }

  public void evict() {
    while (actualSize > cacheSize) {
      double maxSize = Integer.MIN_VALUE;
      long maxUserId = -1;
      for (long userId : actualEvictContext.keySet()) {
        BaseEvictContext context = actualEvictContext.get(userId);
        double usedSize = context.mCacheSize;
        if (usedSize > maxSize) {
          maxSize = usedSize;
          maxUserId = userId;
        }
      }
      // System.out.println("evict " + maxUserId);
      TmpCacheUnit unit = actualEvictContext.get(maxUserId).getEvictUnit();
      actualSize -= actualEvictContext.get(maxUserId).remove(unit);
      checkRemoveByShare(unit, maxUserId);
    }
  }

  public static void main(String[] args) {
    MMFEvictor test = new MMFEvictor(new ClientCacheContext(false));
    test.testUserNum_5();
  }
}
