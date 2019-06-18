package alluxio.client.file.cache.core;

import alluxio.client.file.cache.remote.grpc.service.Data;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;

public class SharedCacheContext implements CacheContext {


  public CacheUnit getCache(long fileId, long begin, long end) {
    return null;
  }

  public Data convertData(ByteBuf data) {
    ByteString entity = ByteString.copyFrom(data.nioBuffer());
    return Data.newBuilder().setData(entity).build();
  }

}
