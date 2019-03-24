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

  private void updateCurrentResponse() {
    mCheckQueueLock.lock();
    if (mCurrResponseIndex != -1) {
      mReceiveData.poll();
    }
    mCheckQueueLock.unlock();
    try {
      mCheckQueueLock.lock();
      while (mReceiveData.isEmpty() || mReceiveData.peek().getPos() != mPos) {
        mCheckQueueLock.unlock();
        Thread.sleep(1);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      mCheckQueueLock.unlock();
      mCurrResponseIndex = 0;
    }
  }


  @Override
  ByteBuf forward() {
    if (mPos == mFileLength) {
      return null;
    }
    if (mCurrentResponse == null || mCurrResponseIndex == -1 ||
        (mCurrResponseIndex == mCurrentResponse.getPayload().size() - 1
        && mCurrBytebufReadedLength == mCurrentBuf.capacity())) {
      updateCurrentResponse();
      mCurrBytebufReadedLength = 0;
      mCurrResponseIndex = 0;
    } else if (mCurrBytebufReadedLength == mCurrentBuf.capacity()){
      mCurrBytebufReadedLength = 0;
      mCurrResponseIndex ++;
    } else {
      throw new RuntimeException("current bytebuf has not finished reading");
    }
    mCurrentBuf = mCurrentResponse.getPayload().get(mCurrResponseIndex);
    return mCurrentBuf;
  }

  @Override
  ByteBuf current() {
    if (mCurrentBuf == null) {
      forward();
    }
    return mCurrentBuf;
  }

  @Override
  public void close() throws IOException {

  }
}
