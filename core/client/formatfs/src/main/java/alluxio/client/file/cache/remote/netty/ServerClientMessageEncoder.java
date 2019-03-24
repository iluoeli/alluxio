package alluxio.client.file.cache.remote.netty;

import alluxio.client.file.cache.remote.netty.message.PayloadMessage;
import alluxio.client.file.cache.remote.netty.message.RPCMessage;
import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.io.IOException;
import java.util.List;

/**
 * RPC message encoder.
 */
public class ServerClientMessageEncoder extends MessageToMessageEncoder<RPCMessage> {

  /**
   * Constructor for {@link ServerClientMessageEncoder}.
   */
  public ServerClientMessageEncoder() {
  }

  @Override
  public void encode(ChannelHandlerContext ctx, RPCMessage msg, List<Object> out) throws Exception {
    RPCMessage.Type type = msg.getType();
    int frameLength = Ints.BYTES; // the frame length itself consists of 4 bytes
    frameLength += type.getEncodedLength();
    frameLength += msg.getEncodedLength();
    List<ByteBuf> payload = null;
    boolean hasPayload = msg instanceof PayloadMessage;
    int dataLen = 0;
    if (hasPayload) {
      try {
        payload = ((PayloadMessage) msg).getPayload();
      } catch (IOException e) {
        throw e;
      }
      for (ByteBuf b : payload) {
        dataLen += b.capacity();
      }
    }
    ByteBuf buf = ctx.alloc().buffer(frameLength - dataLen);
    buf.writeInt(frameLength);
    type.encode(buf);
    msg.encode(buf);

    out.add(buf);
    if (hasPayload) {
      out.addAll(payload);
    }
    System.out.println("encoded : " + msg.toString());
  }
}
