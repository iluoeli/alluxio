package alluxio.client.file.cache.struct;

public class RadixTree<T extends RadixTree.RadixData> {
  private RadixNode mRoot;

  public void add(RadixData data) {
    if (mRoot == null) {
      mRoot = new RadixNode();
    }
    RadixNode tmp = mRoot;
    long begin = data.mBegin;
    while (begin > 0) {
      int index = (int) (begin & 3);
      begin = begin >>> 2;
      if (tmp.son[index] == null) {
        tmp.son[index] = new RadixNode();
      }
      tmp = tmp.son[index];
    }
    tmp.isLeaf = true;
    tmp.data = data;
  }

  public RadixData find(long begin) {
    if (mRoot == null) {
      return null;
    }
    RadixNode tmp = mRoot;
    while (tmp != null && begin > 0) {
      int index = (int)(begin & 3);
      if (tmp.son[index] == null) {
        return null;
      }
      tmp = tmp.son[index];
    }
    if (!tmp.isLeaf) {
      return null;
    }
    return tmp.data;
  }



  class RadixData {
    long mBegin;
    long mLength;
    public RadixData(long begin, long length) {
      mBegin = begin;
      mLength = length;
    }
  }
}
