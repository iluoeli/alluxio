package alluxio.client.file.cache.remote.stream;

import alluxio.client.file.cache.remote.FileCacheContext;
import alluxio.client.file.cache.remote.FileCacheEntity;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.InputStream;

public class CacheFileInputStream extends InputStream {
  private FileCacheContext mCacheContext;
  private FileCacheEntity mData;
  private int mCurrIndex;
  protected int mPos;
  protected int mCurrBytebufReadedLength;
  protected int mFileLength;

  public CacheFileInputStream(long fileId) {
    mCacheContext = FileCacheContext.INSTANCE;
    mData = mCacheContext.getCache(fileId);
    resetIndex();
  }

  public void resetIndex() {
    mCurrIndex = 0;
    mCurrBytebufReadedLength = 0;
  }

  ByteBuf forward() {
    mCurrIndex ++;
    mCurrBytebufReadedLength = 0;
    if (mCurrIndex < mData.mData.size()) {
      return mData.mData.get(mCurrIndex);
    } else {
      return null;
    }
  }

  ByteBuf current() {
    return mData.mData.get(mCurrIndex);
  }

  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(off + len <= b.length);

    int leftToRead = (int) Math.min(mData.getSize() - mPos, len);
    int readedLen = 0;
    ByteBuf current = current();
    int currentBytebyfCanReadLen = current.capacity() - mCurrBytebufReadedLength;

    while (leftToRead > 0) {
      int readLen = Math.min(currentBytebyfCanReadLen, leftToRead);
      current.getBytes(0, b, off + readedLen, readLen);
      leftToRead -= readLen;
      readedLen += readLen;
      mCurrBytebufReadedLength += readedLen;
      mPos += readedLen;
      if (mCurrBytebufReadedLength == current.capacity()) {
        current = forward();
        if (current == null) {
          break;
        }
        currentBytebyfCanReadLen = current.capacity();
      }
    }

    return readedLen == 0? -1: readedLen;
  }


  public int read() throws IOException {
    if (mPos == mData.getFileLength()) {
      return -1;
    }
    ByteBuf current = current();

    int res = current.getByte(mCurrBytebufReadedLength);
    mCurrBytebufReadedLength ++ ;
    mPos ++ ;
    if (mCurrBytebufReadedLength == current.capacity()) {
      forward();
    }
    return res;
  }


  public int positionedRead(byte b[], int pos, int off, int len) throws IOException {
    return mData.positionedRead(b, pos, off, len);
  }

  public void close() throws IOException {

  }

  public int remaining() {
    return mFileLength - mPos;
  }

}
