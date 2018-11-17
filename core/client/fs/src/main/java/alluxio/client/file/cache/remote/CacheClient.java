package alluxio.client.file.cache.remote;

import alluxio.client.file.cache.buffer.MemoryAllocator;
import alluxio.client.file.cache.remote.net.service.Data;
import alluxio.client.file.cache.remote.net.service.DataRequest;
import alluxio.client.file.cache.remote.net.service.DataServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.netty.buffer.ByteBuf;
import org.omg.CORBA.portable.InputStream;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CacheClient {
  private final ManagedChannel channel;
  private final DataServiceGrpc.DataServiceBlockingStub blockingStub;
  private MemoryAllocator memoryAllocator;

  /** Construct client connecting to HelloWorld server at {@code host:port}. */
  public CacheClient(String host, int port,  MemoryAllocator allocator) {
    this(ManagedChannelBuilder.forAddress(host, port)
      .usePlaintext()
      .build());
    memoryAllocator = allocator;
  }

  /** Construct client for accessing HelloWorld server using the existing channel. */
  CacheClient(ManagedChannel channel) {
    this.channel = channel;
    blockingStub = DataServiceGrpc.newBlockingStub(channel);
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  /**
   * Get file data from remote client
   */
  public List<ByteBuf> getDataFromRemote(long fileId, long begin, long end ) {
    DataRequest request = DataRequest.newBuilder().setFileId(fileId).setBegin(begin)
      .setEnd(end).build();
    Iterator<Data> dataIterator = blockingStub.requestData(request);
    List<ByteBuf> res = new ArrayList<>();
    try {
      while (dataIterator.hasNext()) {
        Data currData = dataIterator.next();
        res.addAll(memoryAllocator.copyIntoCache(currData.getData()));
      }
    } catch (StatusRuntimeException e) {
      //TODO handle exception
    }
    return res;
  }
}
