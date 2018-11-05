package alluxio.client.file.cache.struct;

public class LongPair {
  long key;
  long value;

  public long getKey() {
    return key;
  }

  public long getValue() {
    return value;
  }

  public LongPair(long k, long v) {
    key = k;
    value = v;
  }

  public void setValue(long end) {
    value = end;
  }
}
