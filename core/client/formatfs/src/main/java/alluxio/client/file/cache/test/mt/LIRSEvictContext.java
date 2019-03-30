package alluxio.client.file.cache.test.mt;

import alluxio.client.file.cache.ClientCacheContext;
import org.apache.commons.lang3.RandomUtils;

import java.util.*;

public class LIRSEvictContext extends BaseEvictContext {

  private LinkedList<LIRSCacheUnit> LIRList = new LinkedList<>();
  private LinkedList<LIRSCacheUnit> HIRlist = new LinkedList<>();
  private Set<TmpCacheUnit> mAccessSet = new HashSet<>();
  private double mLirSize;

  public LIRSEvictContext(MTLRUEvictor test, ClientCacheContext cacheContext, long userId, long baseSize0) {
    super(test, cacheContext, userId);
    mLirSize = baseSize0;
  }

  public List<TmpCacheUnit> getCacheList() {
    LinkedList<TmpCacheUnit> res = new LinkedList<>();
    res.addAll(HIRlist);
    res.addAll(LIRList);
    return res;
  }

  public void fakeAccess(TmpCacheUnit unit) {
    if (!mAccessSet.contains(unit)) {
      LIRSCacheUnit unit1 = new LIRSCacheUnit(unit);
      if (LIRList.size() < mLirSize) {
        unit1.setStatus(Status.LIR_RESIDENT);
        LIRList.addLast(unit1);
      } else {
        unit1.setStatus(Status.HIR_RESIDENT);
        HIRlist.addLast(unit1);
      }
      mAccessSet.add(unit);
    } else {
      if (LIRList.contains(unit)) {
        LIRSCacheUnit unit1 = new LIRSCacheUnit(unit);
        HIRlist.remove(unit1);
        unit1.setStatus(Status.LIR_RESIDENT);
        LIRList.remove(unit1);
        LIRList.addLast(unit1);
        int index = 0;
        while (LIRList.size() >= mLirSize && index < LIRList.size()) {
          LIRSCacheUnit unit2 = LIRList.get(index);
          if (unit2.getStatus() == Status.LIR_RESIDENT) {
            unit2.setStatus(Status.HIR_RESIDENT);
            LIRList.remove(index);
            HIRlist.addLast(unit2);
          } else  {
            LIRList.remove(index);
          }
          index ++;
        }
      } else {
        LIRSCacheUnit unit1 = new LIRSCacheUnit(unit);
        unit1.setStatus(Status.HIR_RESIDENT);
        LIRList.addLast(unit1);
        HIRlist.addLast(unit1);
      }
    }
  }


  public void fakeRemove(TmpCacheUnit unit){
    mAccessSet.remove(unit);
    HIRlist.remove(unit);
    if (LIRList.contains(unit)) {
      LIRList.remove(unit);
    }
  }

  public TmpCacheUnit getEvictUnit() {
    if (!HIRlist.isEmpty()) {
      return  HIRlist.getFirst();
    } else {
      return LIRList.getFirst();
    }
  }

  public TmpCacheUnit getMaxPriorityUnit() {
    return null;
  }

  public void evict() {
    while (mCacheSize > mCacheCapacity ) {
      TmpCacheUnit deleteUnit = getEvictUnit();
      mCacheSize -= remove(deleteUnit);
    }
  }

  public TmpCacheUnit getSharedEvictUnit() {
    throw new RuntimeException("unsupport");
  //  return getEvictUnit();
  }

  public double getEvictProbability(TmpCacheUnit unit) {
    throw new RuntimeException("unsupport");

  }

  @Override
  public void removeByShare(TmpCacheUnit deleteUnit) {
    if (mAccessSet.contains(deleteUnit)) {
      remove(deleteUnit);
    }
  }

  enum Status  {
    HIR_RESIDENT,
    LIR_RESIDENT,
  }

  private class LIRSCacheUnit extends TmpCacheUnit {
    private Status mStatus;

    public LIRSCacheUnit(TmpCacheUnit unit) {
      super(unit.getFileId(), unit.getBegin(), unit.getEnd());
    }

    public LIRSCacheUnit(long fileId, long begin, long end) {
      super(fileId, begin, end);
    }


    public Status getStatus() {
      return mStatus;
    }

    public void setStatus(Status status) {
      mStatus = status;
    }
  }
}
