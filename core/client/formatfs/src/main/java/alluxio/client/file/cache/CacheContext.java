package alluxio.client.file.cache;

import alluxio.client.file.cache.remote.grpc.service.Data;
import io.netty.buffer.ByteBuf;

public interface CacheContext {


  public CacheUnit getCache(long fileId, long begin, long end);

  public Data convertData(ByteBuf data);

}
