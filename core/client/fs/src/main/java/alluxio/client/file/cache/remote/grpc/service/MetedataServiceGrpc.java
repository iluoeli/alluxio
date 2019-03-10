package alluxio.client.file.cache.remote.grpc.service;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.6.1)",
    comments = "Source: metedataService.proto")
public final class MetedataServiceGrpc {

  private MetedataServiceGrpc() {}

  public static final String SERVICE_NAME = "proto.MetedataService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<QueryRequest,
      QueryResponse> METHOD_IS_EXIST =
      io.grpc.MethodDescriptor.<QueryRequest, QueryResponse>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "proto.MetedataService", "isExist"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              QueryRequest.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              QueryResponse.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<UpdateInfoList,
      UpdateResponse> METHOD_UPDATE_METEDATA =
      io.grpc.MethodDescriptor.<UpdateInfoList, UpdateResponse>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "proto.MetedataService", "updateMetedata"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              UpdateInfoList.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              UpdateResponse.getDefaultInstance()))
          .build();

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static MetedataServiceStub newStub(io.grpc.Channel channel) {
    return new MetedataServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MetedataServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new MetedataServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static MetedataServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new MetedataServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class MetedataServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void isExist(QueryRequest request,
        io.grpc.stub.StreamObserver<QueryResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_IS_EXIST, responseObserver);
    }

    /**
     */
    public void updateMetedata(UpdateInfoList request,
        io.grpc.stub.StreamObserver<UpdateResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_UPDATE_METEDATA, responseObserver);
    }

    @Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_IS_EXIST,
            asyncUnaryCall(
              new MethodHandlers<
                QueryRequest,
                QueryResponse>(
                  this, METHODID_IS_EXIST)))
          .addMethod(
            METHOD_UPDATE_METEDATA,
            asyncUnaryCall(
              new MethodHandlers<
                UpdateInfoList,
                UpdateResponse>(
                  this, METHODID_UPDATE_METEDATA)))
          .build();
    }
  }

  /**
   */
  public static final class MetedataServiceStub extends io.grpc.stub.AbstractStub<MetedataServiceStub> {
    private MetedataServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MetedataServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected MetedataServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MetedataServiceStub(channel, callOptions);
    }

    /**
     */
    public void isExist(QueryRequest request,
        io.grpc.stub.StreamObserver<QueryResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_IS_EXIST, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void updateMetedata(UpdateInfoList request,
        io.grpc.stub.StreamObserver<UpdateResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_UPDATE_METEDATA, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class MetedataServiceBlockingStub extends io.grpc.stub.AbstractStub<MetedataServiceBlockingStub> {
    private MetedataServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MetedataServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected MetedataServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MetedataServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public QueryResponse isExist(QueryRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_IS_EXIST, getCallOptions(), request);
    }

    /**
     */
    public UpdateResponse updateMetedata(UpdateInfoList request) {
      return blockingUnaryCall(
          getChannel(), METHOD_UPDATE_METEDATA, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class MetedataServiceFutureStub extends io.grpc.stub.AbstractStub<MetedataServiceFutureStub> {
    private MetedataServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MetedataServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected MetedataServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MetedataServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<QueryResponse> isExist(
        QueryRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_IS_EXIST, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<UpdateResponse> updateMetedata(
        UpdateInfoList request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_UPDATE_METEDATA, getCallOptions()), request);
    }
  }

  private static final int METHODID_IS_EXIST = 0;
  private static final int METHODID_UPDATE_METEDATA = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final MetedataServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(MetedataServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_IS_EXIST:
          serviceImpl.isExist((QueryRequest) request,
              (io.grpc.stub.StreamObserver<QueryResponse>) responseObserver);
          break;
        case METHODID_UPDATE_METEDATA:
          serviceImpl.updateMetedata((UpdateInfoList) request,
              (io.grpc.stub.StreamObserver<UpdateResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static final class MetedataServiceDescriptorSupplier implements io.grpc.protobuf.ProtoFileDescriptorSupplier {
    @Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return MetedataServiceManager.getDescriptor();
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (MetedataServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new MetedataServiceDescriptorSupplier())
              .addMethod(METHOD_IS_EXIST)
              .addMethod(METHOD_UPDATE_METEDATA)
              .build();
        }
      }
    }
    return result;
  }
}
