package alluxio.client.file.cache.Metric;

import java.util.concurrent.atomic.AtomicLong;

public enum HitRatioMetric {
  INSTANCE;
  public long hitSize;
  public long accessSize;

  public long AddUnitTimeCost;
  public long DeleteUnitTimeCost;
  public long GetUnitTimeCost;

  public long BucketAddTimeCost;
  public long BucketDeleteTimeCost;
  public long BucketFindTimeCost;

  public long TestTimeCost;

  public long ReadUFSTimeCost;
  public AtomicLong ReadUFSCount = new AtomicLong();

  public long LRUChecks;
  public long LRUFliter;

  public double getHitRatio() {
    return (double)hitSize / accessSize;
  }
}
