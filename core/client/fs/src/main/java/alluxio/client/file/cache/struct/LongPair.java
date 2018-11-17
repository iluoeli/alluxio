package alluxio.client.file.cache.struct;

import com.google.common.base.Objects;

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

  @Override
  public int hashCode() {
    return (int)((key * 31 + value) * 31);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof LongPair) {
      LongPair l = (LongPair)obj;
      if (l.getKey() == key && l.getValue() == value) return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return key + " " + value;
  }
}
