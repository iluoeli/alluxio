package alluxio.client.file.cache.remote;

import alluxio.client.file.cache.remote.netty.CacheClient;
import alluxio.client.file.cache.remote.netty.message.RemoteReadResponse;
import alluxio.client.file.cache.remote.stream.RemoteFileInputStream;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public enum FileCacheContext {
  INSTANCE;
  public ExecutorService COMPUTE_POOL = Executors.newFixedThreadPool(4);

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
    System.out.println("get data " + readResponse.toString());
    mDataProducer.get(msgId).consume(readResponse);
  }

  public void finishProduce(long msgId) {
    mDataProducer.get(msgId).isFinishSending.getAndSet(true);
    mDataProducer.remove(msgId);
  }

  public void initProducer(long msgId, RemoteFileInputStream in) {
    mDataProducer.put(msgId, in);
  }
}
