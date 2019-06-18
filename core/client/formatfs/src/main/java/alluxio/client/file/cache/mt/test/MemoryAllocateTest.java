package alluxio.client.file.cache.mt.test;

import alluxio.client.file.cache.buffer.MemoryAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.List;

public class MemoryAllocateTest {
  private MemoryAllocator memoryAllocator = new MemoryAllocator();

  public void test0() {
    List<ByteBuf> l = memoryAllocator.allocate(1024 * 1024 * 423);
    int resSize = 0;
    for(ByteBuf b : l) {
      resSize += b.capacity();

    }
    System.out.println(resSize);
    memoryAllocator.release(l);
  }

  public void testAll() {
    long begin = System.currentTimeMillis();
    List<ByteBuf> l = memoryAllocator.allocate(1024 * 1024 * 1024 + 1024 * 512 );
    int resSize = 0;
    for(ByteBuf b : l) {
      resSize += b.capacity();

    }
   // System.out.println(resSize);
    //memoryAllocator.print();
    System.out.println("allocate time : " + (System.currentTimeMillis() - begin));
    memoryAllocator.release(l);
    //memoryAllocator.print();
  }

  public void testRead() {
    List<ByteBuf> l = memoryAllocator.allocate( 1024 * 512 );
    long begin = System.currentTimeMillis();
    byte b[] = new byte[1024 * 1024];
    System.out.println(l.size());
    for (int i = 0 ;i  <l.size(); i ++) {
      ByteBuf current = l.get(i);
      current.getBytes(0, b, 0, current.capacity());
    }
    System.out.println("read time: " +(System.currentTimeMillis() - begin));
  }

  public void test() {
    memoryAllocator.init();
   // test0();
    testAll();
  }

  public void NettyTest() {
    ByteBuf current = ByteBufAllocator.DEFAULT.heapBuffer( 1024 * 512 );
    long begin = System.currentTimeMillis();
    byte b[] = new byte[1024 * 1024];
    current.getBytes(0, b, 0, current.capacity());

    System.out.println("read time: " +(System.currentTimeMillis() - begin));;
  }



  public static void main(String [] args) {
    MemoryAllocateTest test = new MemoryAllocateTest();
    test.memoryAllocator.init();
    test.testRead();

    //test.testNetty();
  }
}
