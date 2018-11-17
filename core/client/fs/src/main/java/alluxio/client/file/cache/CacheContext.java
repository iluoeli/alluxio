package alluxio.client.file.cache;

import alluxio.client.file.cache.remote.net.service.Data;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;

public interface CacheContext {


  public CacheUnit getCache(long fileId, long begin, long end);

  public Data convertData(ByteBuf data);
}
