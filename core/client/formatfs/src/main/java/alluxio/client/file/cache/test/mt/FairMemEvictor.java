package alluxio.client.file.cache.test.mt;

import alluxio.client.file.cache.ClientCacheContext;

import java.util.*;

public class FairMemEvictor extends MTLRUEvictor {
  private double BAR = 10;
  private Map<Long, BaseAllocateContext> mUserContext = new HashMap<>();
  private int mUserNumber = 3;

  public FairMemEvictor(ClientCacheContext context) {
    super(context);
  }


  public void access( long userId, TmpCacheUnit unit) {
    unit.mCost = ++mCurrentIndex;
    if (!mUserContext.containsKey(userId)) {
      mUserContext.put(userId, new BaseAllocateContext(new LRUEvictContext(this, mContext, userId), cacheSize / mUserNumber));
    }
    BaseAllocateContext userContext = mUserContext.get(userId);
    long newSize = userContext.access(unit);
    if (userContext.getUsedSize() > userContext.getCacheCapacity()) {
      //need evict
      long needEvictSize = userContext.getUsedSize() - userContext.getCacheCapacity();
      userContext.resetCapacity();
      long maxIndex = userId;
      double maxTimeStamp = 0;
      for (long tmpId : mUserContext.keySet()) {
        BaseAllocateContext context = mUserContext.get(tmpId);
        double timeStamp = 0;
        if (context.isDomainUser()) {
          timeStamp = context.getMaxTimeStamp() - mCurrentIndex;
        } else {
          timeStamp = (double) context.getMaxTimeStamp() - mCurrentIndex / (double) BAR;
        }
        if (timeStamp > maxTimeStamp) {
          maxTimeStamp = timeStamp;
          maxIndex = tmpId;
        }
      }
      BaseAllocateContext context = mUserContext.get(maxIndex);
      context.reserCapacity(context.getCacheCapacity() - needEvictSize);
      context.evict();
      actualSize -= needEvictSize;
    }
    mAccessSize += unit.getSize();
    mHitSize += unit.getSize() - newSize;
    actualSize += newSize;
  }

  public static void main(String[] args) {
    FairMemEvictor evictor = new FairMemEvictor(new ClientCacheContext(false));
    evictor.testUserNum_3();
  }

  class BaseAllocateContext {
    private BaseEvictContext mEvictContext;
    private long mSharedSize;

    public BaseAllocateContext(BaseEvictContext context, long shareSize) {
      mEvictContext = context;
      mSharedSize = shareSize;
      mEvictContext.mCacheCapacity = shareSize;
    }

    public void evict() {
      mEvictContext.evict();
    }

    public boolean isDomainUser() {
      return mSharedSize <= mEvictContext.mCacheSize;
    }

    public long getUsedSize() {
      return (long)mEvictContext.mCacheSize;
    }

    public long getCacheCapacity() {
      return mEvictContext.mCacheCapacity;
    }

    public long access(TmpCacheUnit unit) {
      return mEvictContext.access(unit);
    }

    public long getMaxTimeStamp() {
      return mEvictContext.getMaxPriorityUnit().mClientIndex;
    }

    public void resetCapacity() {
      mEvictContext.mCacheCapacity = (long)mEvictContext.mCacheSize;
    }

    public void reserCapacity(long resetSize) {
      mEvictContext.mCacheCapacity = resetSize;
    }
  }
}
