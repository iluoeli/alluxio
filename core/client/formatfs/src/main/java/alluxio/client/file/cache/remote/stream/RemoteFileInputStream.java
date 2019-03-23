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

import static alluxio.client.file.cache.ClientCacheContext.fileId;
import static alluxio.client.file.cache.ClientCacheContext.mCacheSpaceLimit;

public class RemoteFileInputStream extends CacheFileInputStream  {
  private CacheClient mClient;
  private FileCacheContext mContext;
  private PriorityQueue<RemoteReadResponse> mReceiveData;
  private int mCurrBytebufReadedLength;
  private int mCurrResponseIndex;
  private int mPos;

  public RemoteFileInputStream(long fileId) {
    super(fileId);
    String serverHost = mContext.getFileHost(fileId);
    mClient = mContext.getClient(serverHost);
    mReceiveData = new PriorityQueue<>((o1, o2)-> {return o1.getPos() - o2.getPos(); });
  }

  public void receiveData(RemoteReadResponse remoteReadResponse) {
    mReceiveData.offer(remoteReadResponse);
  }


  @Override
  ByteBuf forward() {

  }

  @Override
  ByteBuf current() {

  }

  @Override
  public void close() throws IOException {

  }
}
