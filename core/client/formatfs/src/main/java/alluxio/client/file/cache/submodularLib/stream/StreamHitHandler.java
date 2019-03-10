package alluxio.client.file.cache.submodularLib.stream;

import alluxio.client.file.cache.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class StreamHitHandler {
  private ClientCacheContext mStreamContext;
  private LinkedBlockingQueue<BaseCacheUnit> mVisitQueue;


  public StreamHitHandler() {
    mStreamContext = new ClientCacheContext(false);
    mVisitQueue = new LinkedBlockingQueue<>();
  }

  public void handle(BaseCacheUnit unit, long fileLength) {
    UnlockTask task = new UnlockTask();
    CacheUnit resultUnit = mStreamContext.getCache(unit.getFileId(), fileLength, unit.getBegin(),
      unit.getEnd(), task);
    List<CacheInternalUnit> tmpUnits = new ArrayList<>();
    if (resultUnit.isFinish()) {
      CacheInternalUnit resultUnit0 = (CacheInternalUnit) resultUnit;
      tmpUnits.add(resultUnit0);
    } else {
      TempCacheUnit resultUnit1 = (TempCacheUnit) resultUnit;
      tmpUnits.addAll(resultUnit1.mCacheConsumer);
    }
    double newUnitHit = 1;
    for (CacheInternalUnit resultUnit0 : tmpUnits) {
      for (CacheUnit tmpUnit : resultUnit0.accessRecord) {
        if(unit.getEnd() <= tmpUnit.getBegin()) break;
        if (unit.isCoincience(tmpUnit)) {
          long coinEnd = Math.min(tmpUnit.getEnd(), unit.getEnd());
          long coinBegin = Math.max(tmpUnit.getBegin(), unit.getBegin());
          int coincideSize = (int) (coinEnd - coinBegin);
          double coinPerNew = coincideSize / (double) (unit.getEnd() - unit.getBegin());
          double coinPerOld = coincideSize / (double) (tmpUnit.getEnd() - tmpUnit.getBegin());
          if (coincideSize == tmpUnit.getSize() && coincideSize == unit.getSize()) {
            continue;
          } else {
            newUnitHit += coinPerNew;
            tmpUnit.setCurrentHitVal(tmpUnit.getHitValue() + coinPerOld);
          }
        } else {

        }
      }
    }
    unit.setCurrentHitVal(newUnitHit);
    if (resultUnit.isFinish()) {
      tmpUnits.get(0).accessRecord.add(unit);
    } else {
      TempCacheUnit resultUnit1 = (TempCacheUnit) resultUnit;
      CacheInternalUnit result = mStreamContext.addCache(resultUnit1);
      result.accessRecord.add(unit);
    }
    mVisitQueue.offer(unit);
  }

  public BaseCacheUnit cunsumeUnit() {
    return mVisitQueue.poll();
  }

  public void reservoirSampling() {

  }
}
