package alluxio.client.file.cache.remote;

import alluxio.client.file.cache.remote.netty.CacheClient;
import alluxio.client.file.cache.remote.netty.message.RemoteReadResponse;
import alluxio.client.file.cache.remote.stream.RemoteFileInputStream;
import alluxio.util.io.BufferUtils;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public enum FileCacheContext {
  INSTANCE;
  public ExecutorService COMPUTE_POOL = Executors.newFixedThreadPool(4);
  public static int LOCAL_BUFFER_SIZE = 1024 * 1024;
  private ConcurrentHashMap<Long, FileCacheEntity> mCacheEntity = new ConcurrentHashMap<>();

  private ConcurrentHashMap<Long, RemoteFileInputStream> mDataProducer = new ConcurrentHashMap<>();

  public FileCacheEntity getCache(long fileId) {
    return mCacheEntity.get(fileId);
  }

  public void addCache(FileCacheEntity entity) {
    mCacheEntity.put(entity.getFileId(), entity);
  }

  public String getFileHost(long fileID) {
    return null;
  }

  public ExecutorService getThreadPool() {
    return COMPUTE_POOL;
  }

  public CacheClient getClient(String serverHost) {
     return null;
  }

  public void addCache(long fileId, FileCacheEntity entity) {
    mCacheEntity.put(fileId, entity);
  }

  public void produceData(long msgId, RemoteReadResponse readResponse) {
    mDataProducer.get(msgId).consume(readResponse);
  }

  public void finishProduce(long msgId) {
    mDataProducer.remove(msgId);
  }

  public void addLengthInfo(long msgId, long fileLength) {
    mDataProducer.get(msgId).setFileLength(fileLength);
  }


  public void initProducer(long msgId, RemoteFileInputStream in) {
    mDataProducer.put(msgId, in);
  }

  /**
   * read the data from under fs to local file {@filepath}.
   *
   * @param filePath the local file path
   * @param in the data source
   */
  public void addLocalFileCache(String filePath, InputStream in) throws IOException {

    int readLen = 0;
    byte[] tmp = new byte[LOCAL_BUFFER_SIZE];
    RandomAccessFile cacheFile = new RandomAccessFile(filePath, "rw");
    FileChannel channel = cacheFile.getChannel();
    int pos = 0;

    while ((readLen = in.read(tmp))!=-1) {
      MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE,pos , readLen);
      buffer.put(tmp, 0, readLen);
      BufferUtils.cleanDirectBuffer(buffer);
      pos += readLen;
    }
    in.close();
  }
}
