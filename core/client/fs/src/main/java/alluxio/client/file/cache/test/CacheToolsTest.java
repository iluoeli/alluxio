package alluxio.client.file.cache.test;

import alluxio.client.file.FileInStream;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.*;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import alluxio.wire.FileInfo;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;


public class CacheToolsTest {
  public static URIStatus status = new URIStatus(new FileInfo().setFileId(1).setLength(1000));

  private static class FakeFileInStream extends FileInStream {
  	private long mPosition;
  	private URIStatus mStatus;
    public FakeFileInStream() {
      mPosition = 0;
      mStatus = new URIStatus(new FileInfo().setFileId(1).setLength(100));
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int needRead = Math.min(b.length - off, len);
      for(int i = 0; i < needRead; i ++) {
        b[off + i] = new Long(mPosition ++).byteValue();
      }

      if(needRead <=0 ) {
        needRead = -1;
      }

      return needRead;
    }

    @Override
    public long skip(long n) throws IOException {
      long needSkip = Math.min(n , mStatus.getLength() - mPosition);
      mPosition += needSkip;
      return needSkip;
    }


    @Override
    public long remaining() {
      return mStatus.getLength() - mPosition;
    }
  }


  public static void printAllCacheInfo(URIStatus uri) {
    FileCacheUnit t =  ClientCacheContext.INSTANCE.mFileIdToInternalList.get(uri.getFileId());
    t.print();
  }

  public void printByte(byte [] b) {
    for(int i = 0 ;i < 100 ; i ++) {
      System.out.print(b[i] + " ");
    }
    System.out.println();
  }

  public void readTest() throws IOException{
    byte[] b = new byte[100];
    FileInStreamWithCache in = new FileInStreamWithCache(null,
			ClientCacheContext.INSTANCE, null);
    in.read(b,0,10);

    in.skip(10);

    in.read(b,20,10);

    in.skip(10);
    in.read(b,40,10);


    in.skip(10);
    in.read(b,60,10);


    in.skip(10);
    in.read(b,80,10);

    in.close();

    in = new FileInStreamWithCache(null, ClientCacheContext
			.INSTANCE, null);
    b = new byte[100];
    //in.skip(5);
    in.read(b, 0 ,100);
    printByte(b);
  }

  public static void testLinkBucket() {
    // add cache unit.
    CacheUnit unit = ClientCacheContext.INSTANCE.getCache(status,0 ,10);

    ClientCacheContext.INSTANCE.addCache((TempCacheUnit)unit);
    CacheUnit unit2 = ClientCacheContext.INSTANCE.getCache(status,11 ,20);
    ClientCacheContext.INSTANCE.addCache((TempCacheUnit)unit2);
    CacheUnit unit3 = ClientCacheContext.INSTANCE.getCache(status,21 ,30);
    ClientCacheContext.INSTANCE.addCache((TempCacheUnit)unit3);

    CacheUnit unit4 = ClientCacheContext.INSTANCE.getCache(status,31 ,40);
    ClientCacheContext.INSTANCE.addCache((TempCacheUnit)unit4);


    CacheUnit unit11 = ClientCacheContext.INSTANCE.getCache(status,100 ,110);
    ClientCacheContext.INSTANCE.addCache((TempCacheUnit)unit11);


    CacheUnit unit21 = ClientCacheContext.INSTANCE.getCache(status,111 ,120);
    ClientCacheContext.INSTANCE.addCache((TempCacheUnit)unit21);
    CacheUnit unit31 = ClientCacheContext.INSTANCE.getCache(status,121 ,130);
    ClientCacheContext.INSTANCE.addCache((TempCacheUnit)unit31);

    CacheUnit unit41 = ClientCacheContext.INSTANCE.getCache(status,131 ,140);
    ClientCacheContext.INSTANCE.addCache((TempCacheUnit)unit41);

    printAllCacheInfo(status);


    CacheUnit unitm = ClientCacheContext.INSTANCE.getCache(status,700 ,710);

    ClientCacheContext.INSTANCE.addCache((TempCacheUnit)unitm);

    CacheUnit unitmm = ClientCacheContext.INSTANCE.getCache(status,10 ,520);

    ClientCacheContext.INSTANCE.addCache((TempCacheUnit)unitmm);

    printAllCacheInfo(status);
    Stack<Integer> s = new Stack<>();
  }

  /**
   * Test if data copy happened when cache convert
   */
  public static void testCacheConvert() {
    byte [] tmp = "test".getBytes();
    ByteBuf b =  Unpooled.wrappedBuffer(tmp);
    List<ByteBuf> data = new ArrayList<>();
    data.add(b);
    CacheInternalUnit unit = new CacheInternalUnit(1,2,1, data);
    TempCacheUnit unit2 = new TempCacheUnit(1,2,1);
    unit2.addResource(unit);
    CacheInternalUnit unit22 = unit2.convert();
    if(!tmp.toString().equals(unit22.mData.get(0).array().toString())) {
      System.out.println("copy happened");
    }
    unit.mData.clear();
    unit = null;
    System.out.println(tmp.toString().equals(unit22.mData.get(0).array().toString()));

  }

  public void testDoubeLink() {
    CacheUnit unit = ClientCacheContext.INSTANCE.getCache(status,0 ,10);

    ClientCacheContext.INSTANCE.addCache((TempCacheUnit)unit);
    CacheUnit unit2 = ClientCacheContext.INSTANCE.getCache(status,11 ,20);
    ClientCacheContext.INSTANCE.addCache((TempCacheUnit)unit2);
    CacheUnit unit3 = ClientCacheContext.INSTANCE.getCache(status,25 ,30);
    ClientCacheContext.INSTANCE.addCache((TempCacheUnit)unit3);

    CacheUnit unit4 = ClientCacheContext.INSTANCE.getCache(status,21 ,23);
    ClientCacheContext.INSTANCE.addCache((TempCacheUnit)unit4);

    //ClientCacheContext.INSTANCE.printInfo(status);
    //  CacheUnit unit4 = ClientCacheContext.INSTANCE.getCache(status,12 ,32);
    // ClientCacheContext.INSTANCE.convertCache((TempCacheUnit)unit4);
    printAllCacheInfo(status);
  }

  public void testCacheSplit() {
    CacheInternalUnit unit = new CacheInternalUnit(0,60, 1);
    byte [] b = new byte[60];
    for(int i = 0 ; i < 60 ; i++) {
      b[i] = new Integer(i).byteValue();
    }
    unit.mData.add(Unpooled.wrappedBuffer(b, 0, 10));
    unit.mData.add(Unpooled.wrappedBuffer(b, 10,15));
    unit.mData.add(Unpooled.wrappedBuffer(b, 25,15));
    unit.mData.add(Unpooled.wrappedBuffer(b, 40, 10));
    unit.mData.add(Unpooled.wrappedBuffer(b, 50, 10));

    CacheSet input = new CacheSet();
    input.add(new BaseCacheUnit(5,15,1));
    input.add(new BaseCacheUnit(30,45, 1));
    input.add(new BaseCacheUnit(10,40, 1));
    input.add(new BaseCacheUnit(55,58, 1));

    FileCacheUnit f = new FileCacheUnit(1, 100, null);
    ClientCacheContext.INSTANCE.mFileIdToInternalList.put(1l,f);
    f.getCacheList().add(unit);
    f.elimiate(input);
    f.print();
    //unit.printtest();
  }

  public void just(LinkedList<String> l) {
    l.add("2");
  }

  public void test(LinkedList<String> l, int j) {
    l.add("3");
    if(j == 3) return;
    for(int i =0 ; i < 3; i ++) {
      test(l,j ++);
    }
  }

  public void RBTreeTest() {
		FileCacheUnit f = new FileCacheUnit(1, 1000, null);
		ClientCacheContext.INSTANCE.mFileIdToInternalList.put(1l,f);
		for( int i = 0 ; i < 20; i++) {
			int begin = 100 + i * 5 + 1;
			int end = 105 + i * 5;
			TempCacheUnit unit = (TempCacheUnit) ClientCacheContext.INSTANCE
				.getCache(status, begin, end);
			System.out.println(unit);
			f.addCache(unit);
		}
		for(int i = 0 ; i < 20; i++) {
			int begin = 100 + i * 5 + 1;
			int end = 105 + i * 5;
			CacheInternalUnit unit = (CacheInternalUnit) ClientCacheContext.INSTANCE
				.getCache(status, begin, end);
		}
		for(int i = 0 ; i < 20; i++) {
			int begin = 100 + i * 5 + 1;
			int end = 105 + i * 5;
			CacheInternalUnit unit = (CacheInternalUnit) ClientCacheContext.INSTANCE
				.getCache(status, begin, end);
			System.out.println(unit);
			ClientCacheContext.INSTANCE.mFileIdToInternalList.get(1l).mBuckets
				.delete(unit);
		}

	}

  public static void main(String[] Args) throws IOException{
    LinkedList<String> l= new LinkedList<>();
    new CacheToolsTest().just(l);
    System.out.println(l.toString());

  }
}
