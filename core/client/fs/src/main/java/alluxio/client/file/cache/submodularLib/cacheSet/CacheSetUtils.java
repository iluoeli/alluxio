package alluxio.client.file.cache.submodularLib.cacheSet;

import alluxio.client.file.cache.CacheUnit;
import alluxio.client.file.cache.submodularLib.SubmodularSetUtils;
import com.google.common.base.Preconditions;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;


/**
 * This class only handle set which type is CacheSet and element is CacheUnit
 */

public class CacheSetUtils extends SubmodularSetUtils<CacheUnit> {

  @Override
  public Set<CacheUnit> union(Set<CacheUnit> set, CacheUnit cacheUnit) {
    Preconditions.checkArgument(set instanceof CacheSet);
    CacheSet cacheMap = new CacheSet();


    for (Map.Entry entry : ((CacheSet) set).cacheMap.entrySet()) {
      long fileId = (long) entry.getKey();
      Set<CacheUnit> tmpSet = (Set) entry.getValue();
      cacheMap.put(fileId, (Set) (((TreeSet<CacheUnit>) tmpSet).clone()));
      if (fileId == cacheUnit.getFileId() && !tmpSet.contains(cacheUnit)) {
        cacheMap.get(fileId).add(cacheUnit);
      }
    }
    if (!cacheMap.containsKey(cacheUnit.getFileId())) {
      Set<CacheUnit> set1 = new TreeSet<>(new Comparator<CacheUnit>() {
        @Override
        public int compare(CacheUnit o1, CacheUnit o2) {
          return (int) (o1.getBegin() - o2.getBegin());
        }
      });
      set1.add(cacheUnit);
      cacheMap.put(cacheUnit.getFileId(), set1);
    }

    return cacheMap;
  }

  private Set<CacheUnit> subtract0(Set<CacheUnit> s1, Set<CacheUnit> s2) {
    Set<CacheUnit> res = new TreeSet<>(new Comparator<CacheUnit>() {
      @Override
      public int compare(CacheUnit o1, CacheUnit o2) {
        return (int) (o1.getBegin() - o2.getBegin());
      }
    });
    if ((s2 == null || s1 == null) || s2.isEmpty() || s1.isEmpty()) {
      return res;
    }
    for (CacheUnit unit : s1) {
      if (!s2.contains(unit)) {
        res.add(unit);
      }
    }

    return res;
  }

  @Override
  public Set<CacheUnit> subtract(Set<CacheUnit> s1, Set<CacheUnit> s2) {
    Preconditions.checkArgument(s1 instanceof CacheSet);
    Preconditions.checkArgument(s2 instanceof CacheSet);
    CacheSet res = new CacheSet();
    if (s2.isEmpty() || s1.isEmpty()) {
      return s1;
    }

    for (long fileId : ((CacheSet) s2).cacheMap.keySet()) {
      Set<CacheUnit> set1 = subtract0(((CacheSet) s1).get(fileId), ((CacheSet) s2).get(fileId));
      res.put(fileId, set1);
    }
    for (long fileId : ((CacheSet) s1).cacheMap.keySet()) {
      if (!res.containsKey(fileId)) {
        res.put(fileId, ((CacheSet) s1).get(fileId));
      }
    }
    return res;
  }

  @Override
  public Set<CacheUnit> subtract(Set<CacheUnit> set, CacheUnit cacheUnit) {
    Preconditions.checkArgument(set instanceof CacheSet);

    CacheSet cacheMap = ((CacheSet) set).copy();
    cacheMap.remove(cacheUnit);
    return cacheMap;
  }
}
