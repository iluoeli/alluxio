package alluxio.client.file.cache;

public interface PreviousIterator<E> {

  public boolean hasPrevious();

  public E previous();

  public E getBegin();
}
