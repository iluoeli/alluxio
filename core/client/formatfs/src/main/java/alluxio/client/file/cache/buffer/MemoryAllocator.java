package alluxio.client.file.cache.buffer;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;

import java.util.*;

public class MemoryAllocator {
  private PageAllocator[] allocators;
  private int[] mPageSizeSet;
  private Map<Integer, Integer> mPage2IndexMap;

  public List<ByteBuf> copyIntoCache(ByteString remoteData) {
    int left = remoteData.size();
    List<ByteBuf> allocateRes = allocate(left);
    int index = 0;
    int resourceIndex = 0;
    while (resourceIndex < left) {
      ByteBuf tmpBuf = allocateRes.get(index);
      int capacity = tmpBuf.capacity();
      remoteData.copyTo(tmpBuf.array(), resourceIndex, 0, capacity);
      resourceIndex += capacity;
    }
    return allocateRes;
  }


  public List<ByteBuf> allocateDirect(int size, boolean isNew) {
    //TODO add shared memory allocate part
    return null;
  }

  public void init() {
    allocators = new PageAllocator[3];
    mPageSizeSet = new int[3];
    mPageSizeSet[0] = 1024 * 1024;
    mPageSizeSet[1] = 1024 * 4;
    mPageSizeSet[2] = 512;
    mPage2IndexMap = new HashMap<>();
    allocators[0] = new PageAllocator(512, mPageSizeSet[0]);
    allocators[1] = new PageAllocator(1024 * 128, mPageSizeSet[1]);
    allocators[2] = new PageAllocator(1024, mPageSizeSet[2]);

    mPage2IndexMap.put(1024 * 1024, 0);
    mPage2IndexMap.put(1024 * 4, 1);
    mPage2IndexMap.put(512, 2);

    for (PageAllocator allocator : allocators) {
      allocator.init();
    }

  }

  public List<ByteBuf> allocate(int size) {
    List<ByteBuf> l = allocate0(size, false);
    int i = 0;
    return l;
  }

  public List<ByteBuf> allocate0(int size, boolean isNew) {
    int index = 0;
    int left = size;
    List<ByteBuf> res0 = new ArrayList<>();
    while (index < mPageSizeSet.length && left > 0) {
      int currPageSize = mPageSizeSet[index];
      if (left / currPageSize != 0) {
        List<ByteBuf> tmpRes;
        if (isNew) {
          tmpRes = allocators[index].allocateNew(left / currPageSize);

        } else {
          AllocateResult res = allocators[index].allocate(left / currPageSize);
          tmpRes = res.getRes();
        }

        res0.addAll(tmpRes);
        left -= tmpRes.size() * currPageSize;
      }
      index++;
    }
    if (left > 0 && !isNew) {
      res0.addAll(allocate0(left, true));
    } else if (left > 0) {
      res0.add(ByteBufAllocator.DEFAULT.directBuffer(left));
    }
    return res0;
  }

  public void release(ByteBuf buf) {
    boolean test = false;
    int cap = buf.capacity();
    if (mPage2IndexMap.containsKey(cap)) {
      int index = mPage2IndexMap.get(cap);
      allocators[index].release(buf);
    } else {
      ReferenceCountUtil.release(buf);
    }
  }

  public void release(List<ByteBuf> buf) {
    for (ByteBuf b : buf) {
      release(b);
    }
  }

  public void print() {
    for (PageAllocator allocator : allocators) {
      allocator.print();
    }
  }

  public class PageAllocator {

    public LinkedList<ByteBuf> mPages = new LinkedList<>();
    private int mPageNum;
    private int mPageSize;

    public void print() {
      System.out.print("page size: " + mPageSize + " page num : " + mPages.size() + " all: ");
      int res = 0;
      for (ByteBuf b : mPages) {
        res += b.capacity();
      }
      System.out.println(res);
    }

    public PageAllocator(int pageNum, int pageSize) {
      mPageNum = pageNum;
      mPageSize = pageSize;
    }

    public void init() {
      for (int i = 0; i < mPageNum; i++) {
        mPages.add(ByteBufAllocator.DEFAULT.directBuffer(mPageSize).retain(1));
      }
    }

    public AllocateResult allocate(int pageNum) {
      List<ByteBuf> res = new ArrayList<>();
      for (int i = 0; i < pageNum; i++) {
        if (mPages.size() == 0) {
          return new AllocateResult(res, (pageNum - res.size()) * mPageSize);
        }
        res.add(mPages.pollFirst());
      }
      return new AllocateResult(res);
    }

    public List<ByteBuf> allocateNew(int pageNum) {
      List<ByteBuf> res = new ArrayList<>();
      for (int i = 0; i < pageNum; i++) {
        res.add(ByteBufAllocator.DEFAULT.heapBuffer(mPageSize));
      }
      return res;
    }

    public void release(ByteBuf buf) {
      buf.clear();
      mPages.push(buf);
    }
  }

  public class AllocateResult {
    boolean isSucceed;
    private List<ByteBuf> res;
    private int mLeft;

    public AllocateResult(List<ByteBuf> l) {
      res = l;
      isSucceed = true;
      mLeft = 0;
    }

    public AllocateResult(List<ByteBuf> l, int left) {
      res = l;
      mLeft = left;
      isSucceed = false;
    }

    public List<ByteBuf> getRes() {
      return res;
    }

    public boolean isSucceed() {
      return isSucceed;
    }

    public int getLeft() {
      return mLeft;
    }
  }
}
