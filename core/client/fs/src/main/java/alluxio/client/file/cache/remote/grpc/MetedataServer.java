package alluxio.client.file.cache.remote.grpc;

import alluxio.client.file.cache.*;
import alluxio.client.file.cache.remote.grpc.service.*;
import com.google.common.base.Objects;
import io.grpc.stub.StreamObserver;

import java.util.Map;

public class MetedataServer {
  private Map<Address, ClientCacheContext> mClientContexts;

  private  class MetedataService extends MetedataServiceGrpc.MetedataServiceImplBase {
    @Override
    public void isExist(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
       long fileId = request.getFileId();
       long begin = request.getBegin();
       long end = request.getEnd();
       QueryResponse.Builder responseBuilder = QueryResponse.newBuilder();
       for(Address client : mClientContexts.keySet()) {
         ClientCacheContext tmpContext = mClientContexts.get(client);
         CacheUnit unit = tmpContext.getCache(fileId, begin, end);
         if (unit.isFinish()) {
           responseBuilder.setIp(client.mHost).setPort(client.mPort).setIsExist(true);
           responseObserver.onNext(responseBuilder.build());
           responseObserver.onCompleted();
           return;
         }
       }
       responseObserver.onNext(responseBuilder.setIsExist(false).build());
       responseObserver.onCompleted();
    }

    @Override
    public void updateMetedata(UpdateInfoList request, StreamObserver<UpdateResponse> responseObserver) {
      for(UpdateInfo info : request.getInfosList()) {
        Address address = new Address(info.getHost(), info.getPort());
        ClientCacheContext context = mClientContexts.getOrDefault(address, new ClientCacheContext(false));
        fileInfo info1 = info.getInfo();
        long fileId = info1.getFileId();
        long begin = info1.getBegin();
        long end = info1.getEnd();
        LockTask task = new LockTask(context.getLockManager(), fileId);
        CacheUnit unit = context.getCache(fileId, info1.getFileLength(), begin, end, task);
        if (!unit.isFinish()) {
          context.addCache((TempCacheUnit)unit);
        }
      }
    }
  }

  private class Address {
    String mHost;
    int mPort;

    public Address(String host, int port) {
      mHost = host;
      mPort = port;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Address)) {
        return false;
      }
      Address that = (Address) o;
      return mHost.equals(that.mHost)
        && mPort == that.mPort;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mHost, mPort);
    }

  }


}
