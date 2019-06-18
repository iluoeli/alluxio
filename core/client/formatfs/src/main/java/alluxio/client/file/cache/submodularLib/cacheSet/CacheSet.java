package alluxio.client.file.cache.submodularLib.cacheSet;

import alluxio.client.file.cache.core.BaseCacheUnit;
import alluxio.client.file.cache.core.CacheUnit;
import alluxio.client.file.cache.struct.DoubleLinkedList;
import alluxio.client.file.cache.struct.LongPair;
import com.google.common.base.Preconditions;

import java.util.*;

public class CacheSet implements Set<CacheUnit> {

  public Map<Long, Set<CacheUnit>> cacheMap = new HashMap<>();
  ;
  public DoubleLinkedList<BaseCacheUnit> accessList = new DoubleLinkedList<>(new BaseCacheUnit(-1, -1, -1));
  public Map<Long, PriorityQueue<CacheUnit>> sortCacheMap = new HashMap<>();

  @Override
  public Spliterator spliterator() {
    return null;
  }

  public Map<Long, List<LongPair>> convertFromSort() {
    Preconditions.checkNotNull(sortCacheMap);
    Map<Long, List<LongPair>> res = new HashMap<>();
    for (Map.Entry entry : sortCacheMap.entrySet()) {
      long fileId = (long) entry.getKey();
      Queue<CacheUnit> q = (Queue) entry.getValue();
      LinkedList<LongPair> l = new LinkedList<>();

      long maxEnd = -1;
      long minBegin = -1;
      while (!sortCacheMap.isEmpty()) {
        CacheUnit tmpUnit = q.poll();
        if (minBegin == -1) {
          minBegin = tmpUnit.getBegin();
          maxEnd = tmpUnit.getEnd();
        } else {
          if (tmpUnit.getBegin() <= maxEnd) {
            maxEnd = Math.max(tmpUnit.getEnd(), maxEnd);
          } else {
            l.add(new LongPair(minBegin, maxEnd));
            minBegin = tmpUnit.getBegin();
            maxEnd = tmpUnit.getEnd();
          }
        }
      }
      l.add(new LongPair(minBegin, maxEnd));
      res.put(fileId, l);
    }
    return res;
  }

  public void convertSort() {
    for (long fileId : cacheMap.keySet()) {
      PriorityQueue<CacheUnit> queue;
      if (!sortCacheMap.containsKey(fileId)) {
        //Set<CacheUnit> set = Sets.newLinkedHashSet();\
        queue = new PriorityQueue<>(new Comparator<CacheUnit>() {
          @Override
          public int compare(CacheUnit o1, CacheUnit o2) {
            return (int) (o1.getBegin() - o2.getBegin());
          }
        });
        sortCacheMap.put(fileId, queue);
      } else {
        queue = sortCacheMap.get(fileId);
      }
      queue.addAll(cacheMap.get(fileId));
    }
  }

  @Override
  public boolean isEmpty() {
    return cacheMap.isEmpty();
  }

  public boolean addSort(CacheUnit unit) {
    Long fileId = unit.getFileId();
    if (!sortCacheMap.containsKey(fileId)) {
      //Set<CacheUnit> set = Sets.newLinkedHashSet();\
      PriorityQueue<CacheUnit> queue = new PriorityQueue<>(new Comparator<CacheUnit>() {
        @Override
        public int compare(CacheUnit o1, CacheUnit o2) {
          return (int) (o1.getBegin() - o2.getBegin());
        }
      });
      sortCacheMap.put(fileId, queue);
    }
    return sortCacheMap.get(fileId).add(unit);
  }

  public void sortConvert() {
    sortCacheMap = new HashMap<>();
    for (long fileId : cacheMap.keySet()) {
      PriorityQueue<CacheUnit> queue = new PriorityQueue<>(new Comparator<CacheUnit>() {
        @Override
        public int compare(CacheUnit o1, CacheUnit o2) {
          return (int) (o1.getBegin() - o2.getBegin());
        }
      });
      Set<CacheUnit> s = cacheMap.get(fileId);
      queue.addAll(s);
      sortCacheMap.put(fileId, queue);
    }
  }


  @Override
  public int size() {
    int sum = 0;
    for (Map.Entry entry : cacheMap.entrySet()) {
      sum += ((Set) entry.getValue()).size();
    }
    return sum;
  }

  @Override
  public boolean contains(Object o) {
    if (o instanceof CacheUnit) {
      CacheUnit u = (CacheUnit) o;
      long fileId = u.getFileId();
      if (cacheMap.containsKey(fileId)) {
        return cacheMap.get(fileId).contains(o);
      }
    }
    return false;
  }

  public boolean containsKey(long fileId) {
    return cacheMap.containsKey(fileId);
  }

  public Set<CacheUnit> get(long fileId) {
    return cacheMap.get(fileId);
  }

  public void put(long fileId, Set<CacheUnit> set) {
    cacheMap.put(fileId, set);
  }

  @Override
  public Object[] toArray() {
    Object[] result = new Object[0];
    for (Map.Entry entry : cacheMap.entrySet()) {
      Set set = (Set) entry.getValue();
      Object[] tmp = set.toArray();
      Arrays.copyOf(result, result.length + tmp.length);
      System.arraycopy(tmp, 0, result, result.length, tmp.length);
      result = tmp;
    }
    return result;
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return null;
  }

  @Override
  public boolean add(CacheUnit cacheUnit) {
    Long fileId = cacheUnit.getFileId();
    if (cacheMap.containsKey(fileId)) {
      return cacheMap.get(fileId).add(cacheUnit);
    } else {
      //Set<CacheUnit> set = Sets.newLinkedHashSet();\
      TreeSet<CacheUnit> set = new TreeSet<>(new Comparator<CacheUnit>() {
        @Override
        public int compare(CacheUnit o1, CacheUnit o2) {
          return (int) (o1.getBegin() - o2.getBegin());
        }
      });
      set.add(cacheUnit);
      cacheMap.put(fileId, set);
    }
    return true;
  }

  @Override
  public boolean remove(Object o) {
    if (o instanceof CacheUnit) {
      CacheUnit u = (CacheUnit) o;
      long fileId = u.getFileId();
      if (cacheMap.containsKey(fileId)) {
        Set<CacheUnit> t = cacheMap.get(fileId);
        t.remove(o);
        if (t.isEmpty()) {
          cacheMap.remove(fileId);
        }
      }
    }
    return false;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return false;
  }

  @Override
  public boolean addAll(Collection<? extends CacheUnit> c) {
    return false;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return false;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return false;
  }

  @Override
  public void clear() {
    for (Long l : cacheMap.keySet()) {
      cacheMap.get(l).clear();
    }
    cacheMap.clear();
    for (Long l : sortCacheMap.keySet()) {
      sortCacheMap.get(l).clear();
    }
    sortCacheMap.clear();
  }

  public CacheSet copy() {
    CacheSet res = new CacheSet();
    for (Map.Entry entry : cacheMap.entrySet()) {
      long fileId = (long) entry.getKey();
      Set<CacheUnit> tmp = (Set<CacheUnit>) entry.getValue();
      //Set<CacheUnit> tmp2 = Sets.newLinkedHashSet();
      Set<CacheUnit> tmp2 = new TreeSet<>(new Comparator<CacheUnit>() {
        @Override
        public int compare(CacheUnit o1, CacheUnit o2) {
          return (int) (o1.getBegin() - o2.getBegin());
        }
      });

      tmp2.addAll(tmp);
      res.put(fileId, tmp2);
    }
    return res;
  }

  public Set<Long> keySet() {
    return cacheMap.keySet();
  }

  @Override
  public String toString() {
    return cacheMap.toString();
  }

  @Override
  public Iterator<CacheUnit> iterator() {
    return new innerIterator();
  }

  public Iterator<CacheUnit> sortIterator() {
    return new sortIterator();
  }

  class sortIterator implements Iterator<CacheUnit> {
    private Iterator<Long> iter = null;
    private Iterator<CacheUnit> inIter = null;

    @Override
    public boolean hasNext() {
      if (sortCacheMap.isEmpty()) {
        return false;
      }
      if (inIter == null) {
        iter = sortCacheMap.keySet().iterator();
        inIter = sortCacheMap.get(iter.next()).iterator();
      }
      if (!inIter.hasNext()) {
        return iter.hasNext();
      }
      return true;

    }

    @Override
    public CacheUnit next() {
      if (inIter == null) {
        iter = sortCacheMap.keySet().iterator();
        inIter = sortCacheMap.get(iter.next()).iterator();
      }
      if (!inIter.hasNext()) {
        if (iter.hasNext()) {
          inIter = sortCacheMap.get(iter.next()).iterator();
        } else {
          return null;
        }
      }
      return inIter.next();
    }
  }

  class innerIterator implements Iterator<CacheUnit> {
    private Iterator<Long> iter = null;
    private Iterator<CacheUnit> inIter = null;

    @Override
    public boolean hasNext() {
      if (cacheMap.isEmpty()) {
        return false;
      }
      if (inIter == null) {
        iter = cacheMap.keySet().iterator();
        inIter = cacheMap.get(iter.next()).iterator();
      }
      if (!inIter.hasNext()) {
        return iter.hasNext();
      }
      return true;

    }

    @Override
    public CacheUnit next() {
      if (inIter == null) {
        iter = cacheMap.keySet().iterator();
        inIter = cacheMap.get(iter.next()).iterator();
      }
      if (!inIter.hasNext()) {
        if (iter.hasNext()) {
          inIter = cacheMap.get(iter.next()).iterator();
        } else {
          return null;
        }
      }
      return inIter.next();
    }
  }

}
