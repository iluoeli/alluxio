package alluxio.client.file.cache.remote;

import alluxio.client.file.cache.remote.netty.CacheClient;
import alluxio.collections.ConcurrentHashSet;
import alluxio.wire.WorkerNetAddress;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public enum FileCacheContext {
  INSTANCE;
  public ExecutorService COMPUTE_POOL = Executors.newFixedThreadPool(4);

  private ConcurrentHashMap<Long, FileCacheEntity> mCacheEntity = new ConcurrentHashMap<>();

  public FileCacheEntity getCache(long fileId) {
    return mCacheEntity.get(fileId);
  }

  public void addCache(FileCacheEntity entity) {
    mCacheEntity.put(entity.getFileId(), entity);
  }

  public String getFileHost(long fileID) {

  }

  public ExecutorService getThreadPool() {
    return COMPUTE_POOL;
  }

  public CacheClient getClient(String serverHost) {

  }
}
