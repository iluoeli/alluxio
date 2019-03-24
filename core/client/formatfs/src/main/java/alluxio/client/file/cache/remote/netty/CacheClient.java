package alluxio.client.file.cache.remote.netty;

import alluxio.client.file.cache.remote.FileCacheContext;
import alluxio.client.file.cache.remote.netty.message.RPCMessage;
import alluxio.client.file.cache.remote.netty.message.RemoteReadRequest;
import alluxio.client.file.cache.remote.netty.message.RemoteReadResponse;
import alluxio.client.file.cache.remote.stream.RemoteFileInputStream;
import alluxio.util.ThreadFactoryUtils;
import com.google.common.base.Preconditions;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.DomainSocketChannel;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Random;
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
      connect(getServerAddress(), 26666);
    }
    return mChannel;
  }

  EventLoopGroup createEventLoopGroup(int numThreads, String threadPrefix) {
    ThreadFactory threadFactory = ThreadFactoryUtils.build(threadPrefix, false);
    // new EpollEventLoopGroup(numThreads, threadFactory);
    return new NioEventLoopGroup(numThreads, threadFactory);

  }

  private InetAddress getServerAddress() {
    InetAddress address;
    try {
      address = InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }    // return new DomainSocketAddress("/tmp/domain");
    return address;
  }

  private void connect(InetAddress address, int port) {
    EventLoopGroup workerGroup = createEventLoopGroup(4, "client-netty-thread-%d");
    Bootstrap bootstrap = createBootstrap(workerGroup, new CacheClientChannelHandler());
    try {
      ChannelFuture future = bootstrap.connect(InetAddress.getLocalHost(), 26666);
      future.get();
      mChannel = future.channel();
    } catch (Exception e) {
      workerGroup.shutdownGracefully();
      throw new RuntimeException(e);
    }
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
    bootstrap.handler(new ChannelInitializer<Channel>() {
      @Override
      public void initChannel(Channel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("Frame decoder", RPCMessage.createFrameDecoder());
        pipeline.addLast("Message encoder", new ServerClientMessageEncoder());
        pipeline.addLast("Message decoder", new ServerClientMessageDecoder());
        pipeline.addLast("Message handler", handler);
      }
    });
    return bootstrap;
  }

  class CacheClientChannelHandler extends ChannelInboundHandlerAdapter {
    FileCacheContext mContext = FileCacheContext.INSTANCE;

    @Override
    public  void channelRead(ChannelHandlerContext channelHandlerContext, Object msg) throws Exception {
      Preconditions.checkArgument(msg instanceof RPCMessage, "The message must be RPCMessage");
      RPCMessage rpcMessage = (RPCMessage) msg;
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
  public static void main(String[] arg) throws Exception{
    CacheClient cacheClient = new CacheClient();
    cacheClient.testRead();
    System.out.println("===============finish===============");
  }
}
