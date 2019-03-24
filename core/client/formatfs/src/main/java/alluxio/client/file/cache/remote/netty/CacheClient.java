package alluxio.client.file.cache.remote.netty;

import alluxio.client.file.cache.remote.FileCacheContext;
import alluxio.client.file.cache.remote.netty.message.RPCMessage;
import alluxio.client.file.cache.remote.netty.message.RemoteReadRequest;
import alluxio.client.file.cache.remote.netty.message.RemoteReadResponse;
import alluxio.client.file.cache.remote.stream.RemoteFileInputStream;
import alluxio.client.file.cache.test.CacheClientServerTest;
import alluxio.util.ThreadFactoryUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.channel.unix.DomainSocketChannel;
import org.apache.commons.lang3.RandomUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;

public final class CacheClient {
  public static final CacheClient INSTANCE = new CacheClient();
  private Channel mChannel = null;
  private final String mServerHostName = "localhost";
  private final int mServerPort = 8080;

  public CacheClient() {

  }

  public void testRead() throws IOException {
    long fileId = 1;
    long msgId = Math.abs(new Random(System.currentTimeMillis()).nextLong());

    RemoteFileInputStream in = new RemoteFileInputStream(fileId, msgId);
    RemoteReadRequest readRequest = new RemoteReadRequest(fileId, 0, 10 * 1024 * 1024, msgId);
    FileCacheContext.INSTANCE.initProducer(msgId, in);
    Channel channel = getChannel();
    channel.writeAndFlush(readRequest);
    System.out.println("=============client send=======" + readRequest.toString());

    byte[] tmp = new byte[1024 * 1024];
    int read = 0;
    while ((read = in.read(tmp, 0, 1024 * 1024) )!= -1) {
      System.out.println("read : " + read);
    }

  }


  public Channel getChannel() {
    if (mChannel == null || !mChannel.isActive()) {
      connect(getServerAddress());
    }
    return mChannel;
  }

  EventLoopGroup createEventLoopGroup(int numThreads, String threadPrefix) {
    ThreadFactory threadFactory = ThreadFactoryUtils.build(threadPrefix, false);
    // new EpollEventLoopGroup(numThreads, threadFactory);
    return new NioEventLoopGroup(numThreads, threadFactory);

  }

  private SocketAddress getServerAddress() {
    return new InetSocketAddress("127.0.0.1", 26666);
    // return new DomainSocketAddress("/tmp/domain");
  }

  private void connect(SocketAddress address) {

    EventLoopGroup workerGroup = createEventLoopGroup(4, "client-netty-thread-%d");
    Bootstrap bootstrap = createBootstrap(workerGroup, new CacheClientChannelHandler());
    ChannelFuture future = bootstrap.connect(address);
    try {
      future.get();
    } catch (InterruptedException | ExecutionException e) {
      workerGroup.shutdownGracefully();
      throw new RuntimeException(e);
    }
    mChannel = future.channel();
  }

  private Class<? extends Channel> getSockChannel() {
    //return EpollDomainSocketChannel.class;
    return NioSocketChannel.class;
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
    FileCacheContext mContext = FileCacheContext.INSTANCE;
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, RPCMessage rpcMessage) throws Exception {

      switch (rpcMessage.getType()) {
        case REMOTE_READ_RESPONSE:
          mContext.produceData(rpcMessage.getMessageId(), (RemoteReadResponse)rpcMessage);
        case REMOTE_READ_FINISH_RESPONSE:
          mContext.finishProduce(rpcMessage.getMessageId());
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

}
