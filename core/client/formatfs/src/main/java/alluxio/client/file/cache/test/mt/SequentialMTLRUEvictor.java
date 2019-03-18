package alluxio.client.file.cache.test.mt;

import alluxio.client.file.cache.*;
import alluxio.client.file.cache.test.LRUEvictor;

import java.util.*;

public class SequentialMTLRUEvictor extends LRUEvictor {
  private Map<Long, LRUEvictContext> baseEvictCotext = new HashMap<>();
  private Map<Long, Integer> missCountMap = new HashMap<>();
  private Map<Long, Integer> hitCountMap = new HashMap<>();
  private TreeSet<TmpCacheUnit> p = new TreeSet<>((o1, o2) -> (int)(o2.mCost - o1.mCost));
  private LinkedList<TmpCacheUnit> accessList = new LinkedList<>();
  private long cachespace;
  private long usedSpace;


  public SequentialMTLRUEvictor(ClientCacheContext context) {
    super(context);
  }

  public double function(double input) {
    //System.out.println(input);
    return input ;
  }

  private void access(TmpCacheUnit unit, long clientId) {
    if (!baseEvictCotext.containsKey(clientId)) {
      ClientCacheContext context = new ClientCacheContext(false);
      baseEvictCotext.put(clientId, new LRUEvictContext(new MTLRUEvictor(context), context, clientId));
    }
    unit.mCost = function(missCountMap.getOrDefault(clientId, 0));
    unit.mClientIndex = clientId;
    LRUEvictContext baseEvictContext = baseEvictCotext.get(clientId);
    CacheUnit unit1 = baseEvictContext.mCacheContext.getCache(unit.getFileId(),mTestFileLength,unit.getBegin(), unit.getEnd(), new UnlockTask());

    if (!unit1.isFinish()) {
      CacheInternalUnit newUnit = mContext.addCache((TempCacheUnit) unit1);
      int missTime = missCountMap.getOrDefault(clientId,0) + 1;
      missCountMap.put(clientId, missTime);
      for (TmpCacheUnit tmp : baseEvictContext.getCacheList()) {
        p.remove(tmp);
        tmp.mCost = tmp.mCost + function(missTime) - function(missTime - 1);
        p.add(tmp);
      }

      accessList.add(unit);
      while(usedSpace + unit.getSize() > cachespace) {
        Iterator<TmpCacheUnit> tmpIter = p.iterator();
        TmpCacheUnit lowest = tmpIter.next();
        p.remove(lowest);
        cachespace -= lowest.getSize();

        accessList.remove(lowest);
        baseEvictCotext.get(lowest.mClientIndex).remove(lowest);
        for (TmpCacheUnit unit2 : accessList) {
          p.remove(unit2);
          unit2.mCost -= lowest.mCost;
          p.add(unit2);
        }
      }
      usedSpace += unit.getSize();
      baseEvictContext.access(unit);
    } else {
      hitCountMap.put(clientId, hitCountMap.getOrDefault(clientId, 0));
    }
  }
}
