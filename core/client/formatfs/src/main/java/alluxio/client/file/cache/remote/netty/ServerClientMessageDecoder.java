package alluxio.client.file.cache.remote.netty;

import alluxio.client.file.cache.remote.netty.message.RPCMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

/**
 * RPC message decoder.
 */
public class ServerClientMessageDecoder extends MessageToMessageDecoder<ByteBuf> {
  /**
   * Constructor for {@link ServerClientMessageDecoder}.
   */
  public ServerClientMessageDecoder() {}

  @Override
  public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    System.out.println("decode");
    out.add(RPCMessage.decodeMessage(in));
  }
}
