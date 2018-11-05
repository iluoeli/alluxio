package alluxio.client.file.cache.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import sun.misc.Unsafe;

import java.util.*;

public enum PageManager {
	INSTANCE;
  private Map<Integer, LinkedList<ByteBuf>> mUnusedPageSpace;
  private int[] mPageLengths;
  private int[] mPageNum;
  private double[] mUtilizateRate;
  // public PageManager() {
  //   mCapacity = 0;
  //}
	static {


	}

  public void allocateMemory() {
    for (int i = 0; i < mPageLengths.length; i ++) {
      int pageLength = mPageLengths[i];
      LinkedList<ByteBuf> list = new LinkedList<>();
      for(int j = 0; j < mPageNum[i]; j ++) {
        ByteBuf directMemory = Unpooled.directBuffer(pageLength);
        list.add(directMemory);
      }
     // mCapacity += pageLength * mPageNum[i];
      mUnusedPageSpace.put(pageLength, list);
    }
  }

  private int getLeftSpace(int index) {
    int res = 0;
    for(int i = index; i < mPageLengths.length; i ++) {
      res += mPageLengths[i] * mPageNum[i];
    }
    return res;
  }

  public List<ByteBuf> getCache(long length) {
    List<ByteBuf> tmpCache = new ArrayList<>();
    long leftLength = length;
    for(int i = 0 ; i < mPageLengths.length; i ++) {
      int currLength = mPageLengths[i];
      LinkedList<ByteBuf> list = mUnusedPageSpace.get(currLength);
      int num = mPageNum[i];
      for(int j = 0; j < num; j ++) {
        if (leftLength >= currLength) {
            tmpCache.add(list.poll());
            mPageNum[i]--;
            leftLength -= currLength;
            //mCapacity -= currLength;
        } else {
          if (((double) leftLength / (double) currLength >= mUtilizateRate[i])
           || leftLength > getLeftSpace(i + 1)  ) {
            list.add(new PartialByteBuf(list.poll(), (int)leftLength));
            mPageNum[i]--;
            leftLength = 0;
            break;
          } else {
            break;
          }
        }
      }
			if (leftLength == 0) break;
    }

    if(leftLength > 0) {
    	//TODO
		}
		return tmpCache;
  }

  public void releaseBuffer(ByteBuf buf) {
  	int len = buf.capacity();
  	if(buf instanceof PartialByteBuf) {
			buf = ((PartialByteBuf)buf).getBuffer();
  		len = buf.capacity();
		}
		mUnusedPageSpace.get(len).push(buf);
	}





 }
