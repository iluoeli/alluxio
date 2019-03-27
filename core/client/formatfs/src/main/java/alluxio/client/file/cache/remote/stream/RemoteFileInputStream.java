package alluxio.client.file.cache.remote.stream;

import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.ClientCacheContext;
import alluxio.client.file.cache.remote.FileCacheContext;
import alluxio.client.file.cache.remote.FileCacheEntity;
import alluxio.client.file.cache.remote.netty.CacheClient;
import alluxio.client.file.cache.remote.netty.message.RemoteReadResponse;
import alluxio.client.file.options.InStreamOptions;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static alluxio.client.file.cache.ClientCacheContext.fileId;
import static alluxio.client.file.cache.ClientCacheContext.mCacheSpaceLimit;

public class RemoteFileInputStream extends CacheFileInputStream  {
  private long messageId;
  private PriorityQueue<RemoteReadResponse> mReceiveData;
  private int mCurrResponseIndex;
  private RemoteReadResponse mCurrentResponse = null;
  private ByteBuf mCurrentBuf;
  private ReentrantLock mCheckQueueLock = new ReentrantLock();

  public RemoteFileInputStream(long fileId, long msgId) {
      super(fileId);
    mReceiveData = new PriorityQueue<>((o1, o2)-> {return o1.getPos() - o2.getPos(); });
    mCurrResponseIndex = -1;
    mPos = 0;
    messageId = msgId;
  }

  public void setFileLength(long fileLength) {
    mFileLength = fileLength;
  }

  public long getMessageId() {
    return  messageId;
  }

  public void consume(RemoteReadResponse remoteReadResponse) {
    mCheckQueueLock.lock();
    try {
      mReceiveData.offer(remoteReadResponse);
    } finally {
      mCheckQueueLock.unlock();
    }
  }

  public int leftToRead(int needRead) {
    return needRead;
  }

  private void updateCurrentResponse() {
    mCheckQueueLock.lock();
    if (mCurrResponseIndex != -1) {
      mReceiveData.poll();
    }
    mCheckQueueLock.unlock();

    try {
      mCheckQueueLock.lock();
      while (mPos != mFileLength &&
        (mReceiveData.isEmpty() || mReceiveData.peek().getPos() != mPos)) {
        mCheckQueueLock.unlock();
        Thread.sleep(1);
        mCheckQueueLock.lock();
      }
      if (mPos == mFileLength ) {
        mCurrResponseIndex = -1;
        mCurrentResponse = null;
      } else {
        mCurrentResponse = mReceiveData.peek();
        mCurrResponseIndex = 0;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      mCheckQueueLock.unlock();
    }
  }


  @Override
  ByteBuf forward() throws IOException {
    if (mCurrentResponse == null || mCurrResponseIndex == -1 ||
        (mCurrResponseIndex == mCurrentResponse.getPayload().size() - 1
        && mCurrBytebufReadedLength == mCurrentBuf.capacity())) {
      updateCurrentResponse();
      mCurrBytebufReadedLength = 0;
    } else if (mCurrBytebufReadedLength == mCurrentBuf.capacity()){
      mCurrBytebufReadedLength = 0;
      mCurrResponseIndex ++;
    } else {
      throw new IOException("current bytebuf has not finished reading");
    }
    if (mCurrResponseIndex == -1) {
      mCurrentBuf = null;
      return null;
    }
    System.out.println(mCurrentResponse);
    mCurrentBuf = mCurrentResponse.getPayload().get(mCurrResponseIndex);
    return mCurrentBuf;
  }

  @Override
  ByteBuf current()  throws IOException {
    if (mCurrentBuf == null) {
      return forward();
    }
    return mCurrentBuf;
  }

  @Override
  public void close() throws IOException {

  }
}
