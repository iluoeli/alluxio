package alluxio.client.file.cache.struct;

public class RadixNode {
  public RadixNode[] son;
  boolean isLeaf;
  RadixTree.RadixData data;
  public RadixNode() {
    son = new RadixNode[4];
  }

  public void setData(RadixTree.RadixData o) {
    data = o;
    isLeaf = true;
  }
}
