package alluxio.client.file.cache.remote;

import alluxio.client.file.cache.CacheContext;
import alluxio.client.file.cache.CacheInternalUnit;
import alluxio.client.file.cache.CacheUnit;
import alluxio.client.file.cache.exception.RemoteDataNotFountException;
import alluxio.client.file.cache.remote.net.service.Data;
import alluxio.client.file.cache.remote.net.service.DataRequest;
import alluxio.client.file.cache.remote.net.service.DataServiceGrpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class CacheServer {

  private Server mCacheServer;
  private CacheContext mCacheContext;
  private int mPort;
  private String mHost;

  public CacheServer(int port, CacheContext context) {
    try {
      mCacheServer = ServerBuilder.forPort(port).
        addService(new CacheService()).build();
      mCacheServer.start();
      mPort = port;
      mHost = InetAddress.getLocalHost().getHostAddress();
      mCacheContext = context;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public int getPort() {
    return mPort;
  }

  public String getHost() {
    return mHost;
  }

  private  class CacheService extends DataServiceGrpc.DataServiceImplBase {
      @Override
      public void requestData(DataRequest request, final StreamObserver<Data> dataStreamObserver) {
        CacheUnit unit = mCacheContext.getCache(request.getFileId(), request.getBegin(), request.getEnd());
        if (!unit.isFinish()) {
          dataStreamObserver.onError(new RemoteDataNotFountException("data not found in remote client"));
        } else {
          List<ByteBuf> res = ((CacheInternalUnit)unit).getAllData();
          for(ByteBuf tmpData : res) {
            dataStreamObserver.onNext(mCacheContext.convertData(tmpData));
          }
          dataStreamObserver.onCompleted();
        }
      }
  }
}
