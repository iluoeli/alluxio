package alluxio.client.file.cache.remote.netty;

import alluxio.client.file.CacheFileSystem;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.cache.CacheUnit;
import alluxio.client.file.cache.ClientCacheContext;
import alluxio.client.file.cache.OnlyReadLockTask;
import alluxio.client.file.cache.TempCacheUnit;
import alluxio.client.file.cache.remote.FileCacheContext;
import alluxio.client.file.cache.remote.FileCacheEntity;
import alluxio.client.file.cache.remote.netty.message.RPCMessage;
import alluxio.client.file.cache.remote.netty.message.RemoteReadFinishResponse;
import alluxio.client.file.cache.remote.netty.message.RemoteReadRequest;
import alluxio.client.file.cache.remote.netty.message.RemoteReadResponse;
import alluxio.client.file.cache.test.CacheClientServerTest;
import alluxio.exception.AlluxioException;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.network.NettyUtils;
import alluxio.wire.WorkerNetAddress;
import com.google.common.base.Preconditions;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.nio.NioEventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.channel.unix.DomainSocketChannel;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadFactory;

public class CacheServer {
  private String mHostname;
  private int mPort;
  private FileCacheContext mCacheContext;
  private ChannelHandler mChannelHandler;
  private int SentLimit;

  public CacheServer(String hostName, int port, FileCacheContext cacheContext) {
    mHostname = hostName;
    mPort = port;
    mCacheContext = cacheContext;
    mChannelHandler = new CacheServerChannelHandler();
  }

  public void launch() throws Exception {
    EventLoopGroup bossGroup = createEventLoopGroup(4, "Server-netty-boss-thread-%d");
    EventLoopGroup workerGroup = createEventLoopGroup(getWorkerThreadNum(),
      "Server-netty-worket-thread-%d");
    ServerBootstrap bootstrap = createBootstrap(bossGroup, workerGroup, mChannelHandler);
    ChannelFuture future = bootstrap.bind(getSocketAddress()).sync();
    mCacheContext.getThreadPool().submit(new CloseFutureSync(future, bossGroup, workerGroup));
  }

  private SocketAddress getSocketAddress() {
    return new DomainSocketAddress("/tmp/domain");
  }

  public int getWorkerThreadNum() {
    return Runtime.getRuntime().availableProcessors() * 2;
  }

  EventLoopGroup createEventLoopGroup(int numThreads, String threadPrefix) {
   // ThreadFactory threadFactory = ThreadFactoryUtils.build(threadPrefix, true);
    return new EpollEventLoopGroup(numThreads);
  }

  private Class<? extends ServerChannel> getServerSocketChannel() {
    return EpollServerDomainSocketChannel.class;
  }

  private ServerBootstrap createBootstrap(EventLoopGroup bossGroup, EventLoopGroup workerGroup,
                                    ChannelHandler channelHandler) {
    ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup).channel(getServerSocketChannel())
      .childHandler(new ChannelInitializer<DomainSocketChannel>() {
        @Override
        public void initChannel(DomainSocketChannel ch) throws Exception {
          ChannelPipeline pipeline = ch.pipeline();
          pipeline.addLast("Frame decoder", RPCMessage.createFrameDecoder());
          pipeline.addLast("Message decoder",new ServerClientMessageDecoder());
          pipeline.addLast("Message encoder", new ServerClientMessageEncoder());
          pipeline.addLast("Channel handler", channelHandler);
        }
      }).childOption(ChannelOption.SO_KEEPALIVE, true);
    return bootstrap;
  }

  /**
   * A {@link CloseFutureSync} syncs the netty channel close future asynchronously.
   */
  class CloseFutureSync implements Runnable {
    private final ChannelFuture mChannelFuture;
    private final EventLoopGroup mBossGroup;
    private final EventLoopGroup mWorkerGroup;

    /**
     * Constructor for {@link CloseFutureSync}.
     *
     * @param channelFuture the channel future
     * @param bossGroup the netty boss group
     * @param workerGroup the netty worker group
     */
    public CloseFutureSync(ChannelFuture channelFuture, EventLoopGroup bossGroup,
                           EventLoopGroup workerGroup) {
      mChannelFuture = channelFuture;
      mBossGroup = bossGroup;
      mWorkerGroup = workerGroup;
    }

    @Override
    public void run() {
      try {
        mChannelFuture.channel().closeFuture().sync();
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        mWorkerGroup.shutdownGracefully();
        mBossGroup.shutdownGracefully();
      }
    }
  }

  public class CacheServerChannelHandler extends ChannelInboundHandlerAdapter {


    public CacheServerChannelHandler() {
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      Preconditions.checkArgument(msg instanceof RPCMessage, "The message must be RPCMessage");
      RPCMessage res = (RPCMessage)msg;
      RPCMessage.Type tp = res.getType();
      switch (tp) {
        case REMOTE_READ_REQUEST:
          handleRemoteReadRequest(ctx, (RemoteReadRequest)res);
          break;
        default:
          throw new IllegalArgumentException(String.format("The request type %s is illegal", tp));
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      System.out.println("server wrong " + cause);
      throw new RuntimeException(cause);
    }

    private void handleRemoteReadRequest(ChannelHandlerContext ctx, RemoteReadRequest readRequest) {
      long fileId = readRequest.getFileId();
      long begin = readRequest.getBegin();
      long end = readRequest.getEnd();
      if (begin == 0) {
        easySentData(fileId, ctx, readRequest.getMessageId());
      } else {
        //todo
      }
    }


    // Send a whole file to client continuously.
    private void easySentData(long fileId, ChannelHandlerContext ctx, long messageId) {
      FileCacheEntity entity = mCacheContext.getCache(fileId);
      int currIndex;
      int currLen = 0;
      int before = 0;
      int pos =0;
      while(before < entity.mData.size() ) {
        for (currIndex = before ;currLen < SentLimit && currIndex < entity.mData.size();
             currIndex ++) {
          currLen += entity.mData.get(currIndex).capacity();
        }
        RemoteReadResponse response = new RemoteReadResponse(messageId,
                entity.mData.subList(before, currIndex), currLen, pos);
        ctx.channel().writeAndFlush(response);
        pos += currLen;
        currLen = 0;
        before = currIndex;
      }

      RemoteReadFinishResponse response = new RemoteReadFinishResponse(true, messageId);
      ctx.channel().writeAndFlush(response);
    }
  }




























}
