package alluxio.client.file.cache.Metric;

public enum HitRatioMetric {
  INSTANCE;
  public long hitSize;
  public long accessSize;

  public double getHitRatio() {
    return (double)hitSize / accessSize;
  }

  public void clear() {
    hitSize = 0;
    accessSize = 0;
  }
}
