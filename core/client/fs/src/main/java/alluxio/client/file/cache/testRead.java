package alluxio.client.file.cache;

import alluxio.AlluxioURI;
import alluxio.Client;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSpaceCalculator;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class testRead {
  public static List<Long> beginList = new ArrayList<>();
  public static boolean isWrite = false;
  public static int allInterruptedTime = 0;
  public static ExecutorService COMPUTE_POOL = Executors.newFixedThreadPool(1);
  public static long time0 = 0;

  public static AlluxioURI writeToAlluxio(String s, String alluxioName) throws Exception {
    AlluxioURI uri = new AlluxioURI(alluxioName);
    FileSystem fs = FileSystem.Factory.get();
    if (fs.exists(uri)) {
      fs.delete(uri);
    }
    FileOutStream out = fs.createFile(uri);
    File f = new File(s);
    BufferedInputStream in = new BufferedInputStream(new FileInputStream(f));
    byte[] b = new byte[1024 * 1024];
    int len = 0;
    long readLen = 0;
    while ((len = in.read(b)) > 0 || readLen < 1024 * 1024 * 1024) {
      out.write(b, 0, len);
      readLen += len;
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
    long begin = System.currentTimeMillis();
    byte[] m = new byte[1024 * 1024];
    for (ByteBuffer btf : list) {
      btf.get(m, 0, Math.min(m.length, btf.capacity()));
      btf.rewind();
    }
    //out.write(System.currentTimeMillis() - begin + "");
    //out.newLine();
    System.out.println("read time: " + (System.currentTimeMillis() - begin));

  }

  public static List<ByteBuffer> mmapTest(String filePath) throws Exception {
    RandomAccessFile mLocalFile = new RandomAccessFile(filePath, "r");
    FileChannel mLocalFileChannel = mLocalFile.getChannel();
    long len = mLocalFile.length();
    int pos = 0;
    List<ByteBuffer> ll = new ArrayList<>();
    while (len > 0) {
      MappedByteBuffer mm = mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY, pos, Math.min(1024 * 1024, len));
      ll.add(mm);
      pos += mm.capacity();
      len -= mm.capacity();
    }
    System.out.println(ll.size());
    return ll;
  }

  public static void promoteHeapTest() {
    COMPUTE_POOL.submit(new Runnable() {
      @Override
      public void run() {
        try {
          List<ByteBuffer> ll = mmapTest("/usr/local/hehe.gz");
          while (true) {
            readFromMMap(ll);
          }
        } catch (Exception e) {

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

  public static void testSpace() {
    CacheSet s = new CacheSet();
    for (int i = 0; i < beginList.size(); i++) {
      long begin11 = beginList.get(i);
      s.add(new BaseCacheUnit(begin11, begin11 + 1024 * 1024, 1));
    }
    CacheSpaceCalculator c = new CacheSpaceCalculator();
    System.out.println("space " + c.function(s));
  }

  public static void promotionTest(String s) throws Exception {
    long begin = System.currentTimeMillis();
    AlluxioURI uri = new AlluxioURI(s);
    FileSystem fs = FileSystem.Factory.get();
    FileInStream in = fs.openFile(uri);
    long fileLength = fs.getStatus(uri).getLength();
    int bufferLenth = 1024 * 1024;
    long beginMax = fileLength - bufferLenth;
    byte[] b = new byte[bufferLenth];
    for (int i = 0; i < 1024; i++) {
      long readBegin;
      //  if(!isWrite) {
      readBegin = RandomUtils.nextLong(0, beginMax);
//        beginList.add(readBegin);
      // } else {
      //    readBegin = beginList.get(i);
      //  }
      in.positionedRead(readBegin, b, 0, bufferLenth);
    }
    //isWrite = true;
    //out.write(System.currentTimeMillis() - begin + "");
    //out.newLine();
    System.out.println(s + "read time: " + (System.currentTimeMillis() - begin));
  }


  public static void positionReadTest() throws Exception {
    //ClientCacheContext.INSTANCE.searchTime = 0;
    long begin = System.currentTimeMillis();
    AlluxioURI uri = new AlluxioURI("/testWriteBig");
    FileSystem fs = FileSystem.Factory.get(true);
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
    //	readFirst
    //if(!isWrite) {

    //  isWrite = true;
    //}
    //File f = new File("/home/innkp/read2");
    //f.delete();
    //OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(f),
    // "gbk");
    //BufferedWriter writer = new BufferedWriter(out);
    // AlluxioURI uri = writeToAlluxio("/usr/local/test.gz",
    //  "/testWriteBig");
    //writeToAlluxio("/usr/local/test.gz",
    //  "/testWriteBig1");
    // promoteHeapTest();
    List<ByteBuffer> ll = mmapTest("/usr/local/test.gz");
    for (int i = 0; i < 30; i++) {
      readFromMMap(ll);
      System.out.print(i);
    }
    //writer.close();
    // FileSystem ff = FileSystem.Factory.get();
    //ff.delete(uri);
    System.out.println("finish===================");
  }

}