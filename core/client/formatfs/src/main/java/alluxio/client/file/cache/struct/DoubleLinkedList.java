/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.cache.struct;

import alluxio.client.file.cache.core.PreviousIterator;

import java.util.Iterator;

public class DoubleLinkedList<T extends LinkNode<T>> implements Iterable {
  public T head = null;

  public T tail = null;
  int size = 0;

  public DoubleLinkedList(T head) {
    this.head = head;
    this.head.after = null;
    this.tail = head;
  }

  public int size() {
    int i = 0;
    Iterator iterator = iterator();
    while (iterator.hasNext()) {
      iterator.next();
      i++;
    }
    return i;
  }

  public void add(T node) {
    if (head.after == null) {
      head.after = node;
      tail = node;
      node.before = head;
      node.after = null;
    } else {
      node.before = tail;
      node.after = null;
      tail.after = node;
      tail = node;
    }
    node.setLast();
    size++;
  }

  /*
  public T insertBefore(T newUnit, T current) {
    // add before head
    if(current != head.after) {
      newUnit.after = current;
      newUnit.before = current.before;
      current.before.after = newUnit;
      current.before = newUnit;
    } else {
      newUnit.after = head;
      newUnit.before = null;
      head.before = newUnit;
      head = newUnit;
    }
    return newUnit;
  }*/

  /**
   * Add after current
   */
  public T insertAfert(T newUnit, T current) {
    if (current == tail) {
      add(newUnit);
    } else {
      newUnit.before = current;
      newUnit.after = current.after;
      current.after.before = newUnit;
      current.after = newUnit;
    }
    return newUnit;
  }

  public T pop() {
    if (tail != head) {
      T current = tail;
      tail = (T) tail.before;
      tail.after = null;
      current.before = null;
      return current;
    } else {
      return null;
    }
  }

  public T poll() {
    if (head != tail) {
      T current = (T) head.after;
      head.after = current.after;
      if (current.after != null) {
        current.after.before = head;
      }
      current.after = current.before = null;
      return current;
    } else {
      return null;
    }
  }

  public T insertBetween(T newUnit, T before, T after) {
    if (before == null && after == null) {
      head.after = newUnit;
      tail = newUnit;
      newUnit.before = head;
      newUnit.after = null;
    } else if (before == null) {
      newUnit.before = head;
      head.after = newUnit;
      newUnit.after = after;
      after.before = newUnit;
    } else if (after == null) {
      newUnit.before = before;
      before.after = newUnit;
      tail = newUnit;
      newUnit.after = null;
    } else {
      newUnit.before = before;
      newUnit.after = after;
      after.before = newUnit;
      before.after = newUnit;
    }
    return newUnit;
  }

  public void remove(T node) {
    node.before.after = node.after;
    if (node.after != null) {
      node.after.before = node.before;
    }
    node.before = node.after = null;
  }

  public void addSubLists(T begin, T end) {
    /*
    if(head == null) {
      head = begin;
      tail = end;
      begin.before = null;
      end.after = null;
    } else {
      begin.before = tail;
      end.after = null;
      tail.after = begin;
      tail = end;
    }
    end.setLast();*/
  }

  @Override
  public String toString() {
    Iterator<T> iter = iterator();
    String res = "";
    while (iter.hasNext()) {
      T current = iter.next();
      if (current == null) break;
      res = res + current.toString() + " ";
    }
    return res;
  }

  public void delete(T node) {
    if (node == null) return;
    node.before.after = node.after;
    if (node.after != null) {
      node.after.before = node.before;
    }
    if (node == tail) {
      tail = (T) node.before;
    }
    node.after = node.before = null;
  }

  public void clear() {
    T node = head;
    while (node != null) {
      T tmp = node;
      node = (T) node.after;
      tmp.before = tmp.after = null;
      tmp = null;
    }
  }

  @Override
  public Iterator<T> iterator() {
    return new InnerIterator();
  }

  public PreviousIterator<T> previousIterator() {
    return new InnerIterator();
  }


  public Iterator<T> partitionItreatior(T begin, T end) {
    return new InnerIterator(begin, end);
  }

  public PreviousIterator<T> partitionPreviousItreatior(T begin, T end) {
    return new InnerIterator(begin, end);
  }

  public class InnerIterator implements Iterator<T>, PreviousIterator<T> {
    T current = null;
    T end = null;
    T begin;

    private InnerIterator() {
      begin = head;
      end = tail;
    }

    InnerIterator(T mBegin, T mEnd) {
      end = mEnd;
      begin = mBegin;
    }

    @Override
    public T next() {
      if (current == null) {
        current = (T) begin.after;
        return current;
      } else if (current != end) {
        current = (T) current.after;
        return current;
      } else {
        return null;
      }
    }

    @Override
    public T previous() {
      if (current == null) {
        current = end;
        return current;
      }
      if (current.before != begin) {
        current = (T) current.before;
        return current;
      } else {
        return null;
      }
    }

    @Override
    public boolean hasNext() {
      if (current == null) {
        return begin.after != null;
      }
      return current != end;
    }

    @Override
    public boolean hasPrevious() {
      if (current == null) {
        return end != null;
      }
      return current.before != begin;
    }

    @Override
    public void remove() {
      current = null;
      begin = null;
      end = null;
    }

    @Override
    public T getBegin() {
      return head;
    }
  }
}
