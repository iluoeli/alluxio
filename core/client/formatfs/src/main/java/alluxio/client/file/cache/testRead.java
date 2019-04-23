package alluxio.client.file.cache;

import alluxio.AlluxioURI;
import alluxio.Client;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.file.*;
import alluxio.client.file.cache.struct.DoubleLinkedList;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSpaceCalculator;
import alluxio.util.io.BufferUtils;
import alluxio.worker.block.io.LocalFileBlockReader;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.commons.lang3.RandomUtils;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static alluxio.client.block.stream.BlockInStream.mmapTime;
import static alluxio.client.file.cache.ClientCacheContext.missTime;
import static alluxio.client.file.cache.ClientCacheContext.timeMap;

public class testRead {
  public static List<Long> beginList = new ArrayList<>();
  public static int allInterruptedTime = 0;
  public static ExecutorService COMPUTE_POOL = Executors.newFixedThreadPool(4);
  public static CountDownLatch tmp = new CountDownLatch(4);
  public static CountDownLatch tmp1 = new CountDownLatch(1);
  public static boolean isWrite = false;
  public static long  actualRead = 0;
  public static long cacheRead = 0;
  public static long tmpRead = 0;


  public static AlluxioURI writeToAlluxio(String s, String alluxioName) throws Exception {
    AlluxioURI uri = new AlluxioURI(alluxioName);
    FileSystem fs =CacheFileSystem.get(false);
    if (fs.exists(uri)) {
      fs.delete(uri);
    }
    FileOutStream out = fs.createFile(uri);
    File f = new File(s);
    byte[] b = new byte[1024 * 1024];
    for (int i = 0 ;i  <b.length; i ++) {
      b[i] = (byte)i;
    }

      long readLen = 0;
      while (readLen < 1024 * 1024 * 1024) {
        out.write(b);
        readLen += b.length;
      }

    out.close();
    return uri;
  }

  public static List<ByteBuf> writeToheap(AlluxioURI uri) throws Exception {
    List<ByteBuf> l = new ArrayList<>();
    FileSystem fs = FileSystem.Factory.get();
    FileInStream in = fs.openFile(uri);
    while (in.remaining() > 0) {
      ByteBuf bb = ByteBufAllocator.DEFAULT.directBuffer((int) Math.min(in.remaining(), 1024 * 1024));
      byte[] b = new byte[(int) Math.min(in.remaining(), 1024 * 1024)];
      in.read(b);
      bb.writeBytes(b);
      b = null;
      l.add(bb);
    }
    in.close();
    return l;
  }

  public static List<ByteBuf> writeToheap2(AlluxioURI uri) throws Exception {
    List<ByteBuf> l = new ArrayList<>();
    FileSystem fs = FileSystem.Factory.get();
    FileInStream in = fs.openFile(uri);
    while (in.remaining() > 0) {
      ByteBuf bb = ByteBufAllocator.DEFAULT.heapBuffer((int) Math.min(in.remaining(), 1024 * 1024));
      byte[] b = new byte[(int) Math.min(in.remaining(), 1024 * 1024)];
      in.read(b);
      bb.writeBytes(b);
      b = null;
      l.add(bb);
    }
    in.close();
    return l;
  }

  public static void readFromMMap(List<ByteBuffer> list) {
    byte[] m = new byte[1024 * 1024];
    long begin = System.currentTimeMillis();

    for (ByteBuffer btf : list) {
      btf.get(m, 0, Math.min(m.length, btf.capacity()));
      btf.rewind();
    }
    //out.write(System.currentTimeMillis() - begin + "");
    //out.newLine();
    System.out.println("read time: " + (System.currentTimeMillis() - begin));

  }

  public static void AlluxioMultiRead(String tmp) throws Exception {
    AlluxioURI uri = new AlluxioURI(tmp);
    FileSystem fs = FileSystem.Factory.get();
    FileInStream in = fs.openFile(uri);
    int bufferLenth = 1024 * 1024;
    COMPUTE_POOL.submit(new Runnable() {
      @Override
      public void run() {
        try {
          byte[] b = new byte[bufferLenth];

          AlluxioURI uri = new AlluxioURI("/testWriteBig1");
          FileSystem fs = FileSystem.Factory.get();
          FileInStream in = fs.openFile(uri);
          long beginMax = in.remaining();
          long begin = System.currentTimeMillis();

          for (int i = 0; i < 1024; ) {
            long readBegin;
            readBegin = RandomUtils.nextLong(0, beginMax);
          // in.positionedRead(readBegin, b, 0, bufferLenth);
          }
          System.out.println(System.currentTimeMillis() - begin);

        } catch (Exception e) {
          throw new RuntimeException(e);
        }

      }
    });
    long beginMax = in.remaining();
    byte[] b = new byte[bufferLenth];
    for (int i2 =0 ; i2 < 100; i2 ++) {
      mmapTime.clear();
      long begin = System.currentTimeMillis();
      for (int i = 0; i < 1024; i++) {
        long readBegin;
        readBegin = RandomUtils.nextLong(0, beginMax);
        in.positionedRead(readBegin, b, 0, bufferLenth);
      }
      System.out.println(System.currentTimeMillis() - begin);
      System.out.println("rpc time " + mmapTime.get(Thread.currentThread().getId()));

    }

  }

  public static List<ByteBuffer> mmapTest(String filePath) throws Exception {
    RandomAccessFile mLocalFile = new RandomAccessFile(filePath, "r");
    FileChannel mLocalFileChannel = mLocalFile.getChannel();
    long len = mLocalFile.length();
    int pos = 0;
    List<ByteBuffer> ll = new ArrayList<>();
    while (len > 0) {
      MappedByteBuffer mm = mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY, pos, Math.min
        (1024 * 32 , len));
      // ByteBuffer mm= ByteBuffer.allocate(1024 *  1024);

      ll.add(mm);
      pos += mm.capacity();
      len -= mm.capacity();
    }
    System.out.println(ll.size());
    return ll;
  }

  public static void promoteHeapTest() {
    List<ByteBuf> tmp = new ArrayList<>();
    for (int i = 0 ; i <1000; i ++) {
      tmp.add(ByteBufAllocator.DEFAULT.directBuffer(1024 * 1024));
    }
    COMPUTE_POOL.submit(new Runnable() {
      @Override
      public void run() {
        byte[] b1= new byte[1024 * 1024];
        while(true) {
          for (ByteBuf b : tmp) {
            b.getBytes(0, b1);
          }

        }
      }
    });
  }

  public static void readOrigin() throws Exception {
    long begin = new Date().getTime();
    AlluxioURI uri = new AlluxioURI("/testWriteBig");
    FileSystem fs = FileSystem.Factory.get();
    FileInStream in = fs.openFile(uri);
    byte[] b = new byte[1024 * 1024];
    int read;
    int l = 0;
    while ((read = in.positionedRead(l, b, 0, b.length)) != -1) {
      l += read;
    }
    long end = System.currentTimeMillis();
    long time = end - begin;
    System.out.println(time);

  }

  public static final int getProcessID() {
    RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    System.out.println(runtimeMXBean.getName());
    return Integer.valueOf(runtimeMXBean.getName().split("@")[0]).intValue();
  }


  public static void getInterruptedTime() {
    int i = getProcessID();
    Runtime run = Runtime.getRuntime();
    try {
      Process process = run.exec("ps -o min_flt,maj_flt " + i);
      InputStream in = process.getInputStream();
      BufferedReader bs = new BufferedReader(new InputStreamReader(in));
      String s = "";
      String res = " ";
      while ((s = bs.readLine()) != null) {
        res = s;
      }
      int res1 = Integer.parseInt(res.split("\\s+")[1]);
      System.out.println("interrupted time : " + (res1 - allInterruptedTime));
      allInterruptedTime = res1;
      in.close();
      process.destroy();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void multiThreadTest () throws Exception {
    System.out.println("================================multi thread test begin");
    FileSystem fs = FileSystem.Factory.get();
    for (int i = 0; i <4; i ++) {
      COMPUTE_POOL.submit(new Runnable() {
        @Override
        public void run() {
          AlluxioURI uri = new AlluxioURI("/" + RandomUtils.nextBytes(1000));
          try(FileOutStream out = fs.createFile(uri) ){
            for (int i = 0 ; i <200 * 1024; i ++ ) {
              out.write(i);
            }
          } catch (Exception e) {

          }
          try {
            FileInStream in = fs.openFile(uri);
            while(in.remaining() > 0) {
              in.read();
            }
          } catch (Exception e) {

          }
        }
      });
    }
    System.out.println("test 1");
    tmp1.countDown();
    System.out.println("test 2");

    tmp.await();

    COMPUTE_POOL.shutdown();
    for (long threadId : timeMap.keySet()) {
      System.out.println("time " + threadId + " " + timeMap.get(threadId));
    }

  }

  public static void testSpace() {
    CacheSet s = new CacheSet();
    for (int i = 0; i < beginList.size(); i++) {
      long begin11 = beginList.get(i);
      s.add(new BaseCacheUnit(begin11, begin11 + 1024 * 1024, 1));
    }
    CacheSpaceCalculator c = new CacheSpaceCalculator();
    System.out.println("space " + c.function(s));
  }


  public static void test1() throws Exception{
    FileSystem fs = CacheFileSystem.get(true);
    final List<ByteBuf> l = new ArrayList<>();

    int index = 0;

    for (int i = 0 ; i <1024; i ++) {
      l.add(ByteBufAllocator.DEFAULT.heapBuffer(1024 * 1024));
      //CacheInternalUnit unit = new CacheInternalUnit(index, index + 1024 * 1024, 0, data);
    }
    for (int i = 0; i <4; i ++) {
      COMPUTE_POOL.submit(new Runnable() {
        @Override
        public void run() {
          byte[] bb = new byte[1024 * 1024];
          ClientCacheContext.timeMap.put(Thread.currentThread().getId(), 0l);
          try {
            long begin = System.currentTimeMillis();
            tmp1.await();
           // int pos = 0;
            for (ByteBuf data : l){
              //CacheInternalUnit unit = iter.next();
              //unit.test(bb)
              data.getBytes(0, bb, 0, data.capacity());
            }
            tmp.countDown();
            System.out.println("time cost: "+ (System.currentTimeMillis() - begin));
          } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        }
      });
    }
    tmp1.countDown();
    long begin = System.currentTimeMillis();
    tmp.await();
    System.out.println("multi thread time: " + (System.currentTimeMillis() - begin ));
    COMPUTE_POOL.shutdown();
  }

  public static void promotionTest(String s) throws Exception {
    long begin = System.currentTimeMillis();
    FileInStream.initmetric();
    actualRead = 0;
    cacheRead = 0 ;
    tmpRead = 0;
    ClientCacheContext.missTime = 0;
    AlluxioURI uri = new AlluxioURI(s);
    FileSystem fs = CacheFileSystem.get(true);
    FileInStream in = fs.openFile(uri);
    long fileLength = fs.getStatus(uri).getLength();
    int bufferLenth = 1024 * 1024;
    long beginMax = fileLength - bufferLenth;
    byte[] b = new byte[bufferLenth];
    int readNum = 0;
    for (int i = 0; i < 1024; i++) {
      long readBegin;
      //if(!isWrite) {
        readBegin = RandomUtils.nextLong(0, beginMax);
       // beginList.add(readBegin);
      //} else {
        //if (readNum < 800) {
         // readBegin = RandomUtils.nextLong(0, beginMax);
        //} else {
         // readBegin = beginList.get(i);
        //}
      //}
      in.positionedRead(readBegin, b, 0, bufferLenth);
      readNum ++;
    }
    isWrite = true;
    //out.write(System.currentTimeMillis() - begin + "");
    //out.newLine();
    System.out.println("------------------");
    System.out.println("actual read : " +actualRead);
    System.out.println("cache read : " + cacheRead);
    System.out.println("tmp read : " + tmpRead);
   System.out.println("------------------");
    System.out.println(Thread.currentThread().getId() + " read time : " + (System
      .currentTimeMillis() - begin));
  }

  public static void positionReadTest() throws Exception {
    // ClientCacheContext.INSTANCE.searchTime = 0;

    long begin = System.currentTimeMillis();
    AlluxioURI uri = new AlluxioURI("/testWriteBig");
    FileSystem fs = CacheFileSystem.get(true);
    FileInStream in = fs.openFile(uri);
    long fileLength = fs.getStatus(uri).getLength();
    int bufferLenth = 1024 * 1024;
    byte[] b = new byte[bufferLenth];
    long beginMax = fileLength - bufferLenth;
    System.out.println("read begin" + Thread.currentThread().getId());
    ClientCacheContext.checkout = 0;
    ClientCacheContext.missSize = 0;
    ClientCacheContext.hitTime = 0;

    if (beginList.size() == 0) {
      for (int i = 0; i < 1024; i++) {
        long readBegin = RandomUtils.nextLong(0, beginMax);
        beginList.add(readBegin);
        in.positionedRead(readBegin, b, 0, bufferLenth);
      }
    } else {
      for (int i = 0; i < 1024; i++) {
        in.positionedRead(beginList.get(i), b, 0, bufferLenth);
      }
    }

    long end = System.currentTimeMillis();
    long time = end - begin;

    System.out.println(time);
    //getInterruptedTime();
    /*
		System.out.println("search : " + (((FileInStreamWithCache)in)
			.mCacheContext.searchTime));
		System.out.println("read : " + ((FileInStreamWithCache)in).mCachePolicy
			.mReadTime);
		System.out.println("break time" + ClientCacheContext.checkout + " hit " +
			"ratio"  +  (1 - ((double)ClientCacheContext.missSize / 1024 / 1024 /
			(double)1024)) +
			" hitTime " +
			ClientCacheContext.hitTime);*/
  }

  public static void test() throws Exception {
    long begin = System.currentTimeMillis();
    FileSystem fs = CacheFileSystem.get(true);
    fs = FileSystem.Factory.get();
    FileInStream in = fs.openFile(new AlluxioURI("/testWriteBig"));
    byte[] b = new byte[1024 *1024];
    int pos = 0;
    while(in.remaining() > 0) {
      int res = in.read( b, 0, (int)Math.min(b.length, in.remaining()));
      pos += res;
    }
    long end = System.currentTimeMillis() - begin;
    System.out.println(end);
  }

  public static void readFirstTime(int l) throws Exception {
    long begin = System.currentTimeMillis();
    AlluxioURI uri = new AlluxioURI("/testWriteBig");
    // FileSystem fs = CacheFileSystem.get();
    FileSystem fs = FileSystem.Factory.get();
    FileInStream in = fs.openFile(uri);
    //	((FileInStreamWithCache)in).mCachePolicy.mReadTime = 0;
    //	ClientCacheContext.INSTANCE.readTime = 0;
    byte[] b = new byte[l];
    int read;
    int ll = 0;
    System.out.println("read begin" + Thread.currentThread().getId());
    while ((read = in.positionedRead(ll, b, 0, b.length)) != -1) {
      ll += read;
    }
		/*
		in = fs.openFile(uri);
		FileInStreamWithCache in2 = (FileInStreamWithCache)in;
		in2.mCachePolicy.clearInputSpace();
	  //b = new byte[2000];
		while ((read = in.read(b))!= -1) {
		}*/

    long end = System.currentTimeMillis();
    long time = end - begin;

    System.out.println(time);
  }


  public static void main(String[] args) throws Exception {
    //writeToAlluxio("/usr/local/test.gz", "/testWriteBig");
    // promoteHeapTest();
    //for (int i =0 ; i < 10; i ++) {
    // test();
    //System.out.println("test");
    // }

    //multiThreadTest();
    for (int i = 0 ;i < 100; i ++) {
     promotionTest("/testWriteBig");
    }
   // AlluxioMultiRead( "/testWriteBig");



   // multiThreadTest();
    //FileSystem fs LocalFilePacketReader= FileSystem.Factory.get();





    System.out.println("====================finish=================");
  }
}