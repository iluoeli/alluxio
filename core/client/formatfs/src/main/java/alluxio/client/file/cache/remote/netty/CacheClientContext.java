package alluxio.client.file.cache.remote.netty;

import alluxio.client.file.cache.remote.netty.message.RPCMessage;
import alluxio.client.file.cache.remote.netty.message.RemoteReadRequest;
import alluxio.client.file.cache.remote.netty.message.RemoteReadResponse;
import alluxio.client.file.cache.test.CacheClientServerTest;
import alluxio.util.ThreadFactoryUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.channel.unix.DomainSocketChannel;

import java.net.InetAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;

public final class CacheClientContext {
  public static final CacheClientContext INSTANCE = new CacheClientContext();
  private Channel mChannel = null;
  private Map<Long, Response> mDataMap = new ConcurrentHashMap<>();
  private final String mServerHostName = "localhost";
  private final int mServerPort = 8080;
  //private boolean supportDomainSocket = true;

  private CacheClientContext() {

  }

  public void addData(long messageID, List<ByteBuf> data) {
    mDataMap.get(messageID).addData(data);
  }

  public List<ByteBuf> read(long fileId, long begin, long end) {
    Response response = new Response();
    RemoteReadRequest readRequest = new RemoteReadRequest(fileId, begin, end);
    long messageId = readRequest.getMessageId();

    mDataMap.put(messageId, response);
    mChannel = getChannel();
    mChannel.writeAndFlush(readRequest);
    return response.get();
  }

  public int read0(List<ByteBuf> src, byte[] b, int off, int len) {
    int pos = off;
    for (ByteBuf tmp : src) {
      tmp.readBytes(b, pos, tmp.capacity());
      pos += tmp.capacity();
    }
    return len;
  }

  public Channel getChannel() {
    if (mChannel == null || !mChannel.isActive()) {
      connect(getServerAddress());
    }
    return mChannel;
  }

  EventLoopGroup createEventLoopGroup(int numThreads, String threadPrefix) {
    ThreadFactory threadFactory = ThreadFactoryUtils.build(threadPrefix, true);
    return new EpollEventLoopGroup(numThreads, threadFactory);
  }

  private SocketAddress getServerAddress() {
    return new DomainSocketAddress("/tmp/domain");
  }

  private void connect(SocketAddress address) {
    EventLoopGroup workerGroup = createEventLoopGroup(1, "client-netty-thread-%d");
    Bootstrap bootstrap = createBootstrap(workerGroup, new CacheClientChannelHandler());
    ChannelFuture future = bootstrap.connect(address);
    try {
      future.get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      workerGroup.shutdownGracefully();
    }
    mChannel = future.channel();
  }

  private Class<? extends Channel> getSockChannel() {
    return EpollDomainSocketChannel.class;
  }

  private Bootstrap createBootstrap(EventLoopGroup workerGroup, final
  ChannelHandler handler) {
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(workerGroup);
    bootstrap.channel(getSockChannel());
    bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
    bootstrap.handler(new ChannelInitializer<DomainSocketChannel>() {
      @Override
      public void initChannel(DomainSocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("Frame decoder", RPCMessage.createFrameDecoder());
        pipeline.addLast("Message decoder", new ServerClientMessageDecoder());
        pipeline.addLast("Message encoder", new ServerClientMessageEncoder());
        pipeline.addLast("Message handler", handler);
      }
    });
    return bootstrap;
  }

  class CacheClientChannelHandler extends SimpleChannelInboundHandler<RPCMessage> {

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, RPCMessage rpcMessage) throws Exception {
      CacheClientServerTest.readTime += (System.currentTimeMillis() - CacheClientServerTest
        .beginTime);

      switch (rpcMessage.getType()) {
        case REMOTE_READ_RESPONSE:
          List<ByteBuf> data =((RemoteReadResponse) rpcMessage).getPayload();
          CacheClientContext.INSTANCE.addData(((RemoteReadResponse) rpcMessage).getMessageId()
            , data);
          break;
        default:
          throw new IllegalArgumentException("Illegal received message type " + rpcMessage.getType());
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      System.out.println("client wrong " + cause);
      throw new RuntimeException(cause);
    }
  }

  class Response {
    List<ByteBuf> mData;
    CountDownLatch mWait;

    public Response() {
      mWait = new CountDownLatch(1);
    }

    public void addData(List<ByteBuf> data) {
      mData = data;
      mWait.countDown();
    }

    public List<ByteBuf> get() {
      try {
        mWait.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return mData;
    }
  }
}
