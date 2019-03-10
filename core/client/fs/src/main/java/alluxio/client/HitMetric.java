package alluxio.client;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.commons.lang3.RandomUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class HitMetric {
  public static long mHitSize;
  public static long mMissSize;
  public static long mAccessSize;
  public static ExecutorService COMPUTE_POOL = Executors.newFixedThreadPool(100);

  public static void main(String [] args) {
    final int bufferLength = 1024 * 16;
    final int threadNUm = 10;
    ConcurrentLinkedQueue<ByteBuf> tmp = new ConcurrentLinkedQueue<>();

    for (int i = 0; i < 1024 * 1024 * 1024 / bufferLength; i++) {
      tmp.add(ByteBufAllocator.DEFAULT.directBuffer(bufferLength));
    }
    for (int i = 0 ; i < threadNUm; i ++) {
      COMPUTE_POOL.submit(new Runnable() {

        @Override
        public void run() {

          byte[] b1 = new byte[bufferLength];

          while (true) {
            for (ByteBuf b : tmp) {
              b.getBytes(0, b1);
            }
            try {
              Thread.sleep(10);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        }
      });
    }

    byte[] b1= new byte[bufferLength];
    long sum = 0 ;
    for (int i = 0; i < 20; i ++) {
      long begin = System.currentTimeMillis();
      for (ByteBuf b : tmp) {
        b.getBytes(0, b1);
      }
      System.out.println("cost : " + (System.currentTimeMillis() - begin));
      if (i >= 10) {
        sum += (System.currentTimeMillis() - begin);
      }
    }
    System.out.println("ave cost " + sum / 10);
    System.out.println("====================finish=================");
    COMPUTE_POOL.shutdownNow();
    }

}
