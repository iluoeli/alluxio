package alluxio.client.file.cache;

import alluxio.client.file.cache.submodularLib.cacheSet.DivideGR;

public interface CachePolicy {
  enum PolicyName {
    LRU, ISK, GR, DIVIDE_GR
  }

  class factory {
    private factory() {}

    public static CachePolicy create(PolicyName name) {
      CachePolicy policy = null;
      switch (name) {
        case GR:
          SKPolicy.INSTANCE.setPolicy(PolicyName.GR);
          policy = SKPolicy.INSTANCE;
          break;
        case LRU:
          policy = new LRUPolicy();
          break;
        case ISK:
          SKPolicy.INSTANCE.setPolicy(PolicyName.ISK);
          policy = SKPolicy.INSTANCE;
          break;
        case DIVIDE_GR:
          policy = new DivideGR();
          break;
        default:
          break;
      }
      return policy;
    }
  }

  public boolean isSync();

  public void init(long cacheSize, ClientCacheContext context);

  public void fliter(CacheInternalUnit unit, BaseCacheUnit unit1);

  public void check(TempCacheUnit unit);

  public long evict();

  public void clear();

  public PolicyName getPolicyName();
}
