package alluxio.client.file.cache.submodularLib;

import java.util.Set;

public abstract class SubmodularSetUtils<T extends Element> {

  public abstract Set<T> union(Set<T> set, T j);

  public abstract Set<T> subtract(Set<T> s1, Set<T> s2);

  public abstract Set<T> subtract(Set<T> s1, T e);

  public boolean approximationEqual(Set<T> s1, Set<T> s2) {
    return s1.size() == s2.size();
  }

}
