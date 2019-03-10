package alluxio.client.file.cache.submodularLib;

import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;

import java.util.Set;

public abstract class IterateOptimizer<T extends Element> {

  public abstract void init();

  public void optimize() {
    init();
    while (!convergence()) {
      iterateOptimize();
    }
  }

  public abstract void iterateOptimize();

  public abstract boolean convergence();

  public abstract Set<T> getResult();

  public abstract void addInputSpace(Set<T> input);

  public abstract void clear();

}
