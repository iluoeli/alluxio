package alluxio.client.file.cache.remote.grpc;

import alluxio.client.file.cache.core.ClientCacheContext;
import alluxio.client.file.cache.buffer.MemoryAllocator;
import alluxio.client.file.cache.remote.grpc.service.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class MetedataClient {

  private final ManagedChannel channel;
  private final MetedataServiceGrpc.MetedataServiceBlockingStub blockingStub;
  private MemoryAllocator memoryAllocator;
  private LinkedBlockingQueue<UpdateInfo> mNeedUpdateUnit = new LinkedBlockingQueue<>();
  private ClientCacheContext mContext;
  private int maxSendLength;
  private String mHost;
  private int mPort;


  /** Construct client connecting to HelloWorld server at {@code host:port}. */
  public MetedataClient(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port)
      // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
      // needing certificates.
      .usePlaintext()
      .build());
  }

  /** Construct client for accessing HelloWorld server using the existing channel. */
  MetedataClient(ManagedChannel channel) {
    this.channel = channel;
    blockingStub = MetedataServiceGrpc.newBlockingStub(channel);
  }

  public void queryMetedata(long fileId, long begin, long end) {
    QueryRequest request = QueryRequest.newBuilder().setFileId(fileId).setBegin(begin).setEnd(end).build();
    QueryResponse response = blockingStub.isExist(request);
  }

  public void addNewMetedata(long fileId, long begin, long end) {
    fileInfo info0 = fileInfo.newBuilder().setFileId(fileId).setBegin(begin).setEnd(end).build();
    addMetedata(info0);
  }

  public void addMetedata(fileInfo info0) {
    UpdateInfo info = UpdateInfo.newBuilder()
      .setInfo(info0).setHost(mHost).setPort(mPort).build();
    mNeedUpdateUnit.add(info);
  }

  private class updateMetedataThread implements Runnable {
    @Override
    public void run() {
      try {
        while (true) {
          List<UpdateInfo> tmpMetedata = new ArrayList<>();
          while (tmpMetedata.size() < maxSendLength) {
            UpdateInfo info = mNeedUpdateUnit.take();
            tmpMetedata.add(info);
          }
          UpdateInfoList sendlist = UpdateInfoList.newBuilder().addAllInfos(tmpMetedata).build();
          UpdateResponse response = blockingStub.updateMetedata(sendlist);
          for(fileInfo info : response.getFailedDataList()) {
            addMetedata(info);
          }
          Thread.sleep(1000);
        }
      } catch (InterruptedException e) {

      }
    }
  }
}
