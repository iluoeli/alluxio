package alluxio.client.file.cache.test.MTTest;

import alluxio.client.file.cache.ClientCacheContext;

public class MMFEvictor extends MTLRUEvictor {

  public MMFEvictor(ClientCacheContext context) {
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
    if (actualSize > cacheSize) {
      evict();
    }
  }

  public void evict() {
    while (actualSize > cacheSize) {
      long maxSize = 0;
      long maxUserId = -1;
      for (long userId : actualEvictContext.keySet()) {
        BaseEvictContext context = actualEvictContext.get(userId);
        long usedSize = context.mCacheSize;
        if (usedSize > maxSize) {
          maxSize = usedSize;
          maxUserId = userId;
        }
      }
      TmpCacheUnit unit = actualEvictContext.get(maxUserId).getEvictUnit();
      actualSize -= actualEvictContext.get(maxUserId).remove(unit);
    }
  }

  public static void main(String[] args) {
    MMFEvictor test = new MMFEvictor(new ClientCacheContext(false));
    test.test();
  }
}
