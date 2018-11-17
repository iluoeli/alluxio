package alluxio.client.file.cache.test;

import alluxio.client.file.cache.buffer.MemoryAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayList;
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

  public void test() {
    memoryAllocator.init();
   // test0();
    testAll();
  }

  public void NettyTest() {
    long begin  = System.currentTimeMillis();
    List<ByteBuf> l = new ArrayList<>();
    for(int i = 0; i < 1025; i ++) {
      ByteBuf b = ByteBufAllocator.DEFAULT.heapBuffer(1024 * 1024);
      l.add(b);
    }
    long tmp = System.currentTimeMillis();
    System.out.println("allocate " + (tmp - begin));
    for(ByteBuf b : l) {
      ReferenceCountUtil.release(b);
    }
    l.clear();
  }

  public void testNetty() {

  }

  public static void main(String [] args) {
    MemoryAllocateTest test = new MemoryAllocateTest();
    test.memoryAllocator.init();
    test.testAll();
    System.out.println(test.memoryAllocator.allocateTime);
    test.memoryAllocator.allocateTime = 0;
    test.testAll();
    System.out.println(test.memoryAllocator.allocateTime);
    test.memoryAllocator.allocateTime = 0;
    test.testAll();
    test.testAll();
    test.testAll();
    test.testAll();

    //test.testNetty();
  }
}
