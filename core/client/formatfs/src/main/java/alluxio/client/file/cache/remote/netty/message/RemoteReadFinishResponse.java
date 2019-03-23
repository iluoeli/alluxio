package alluxio.client.file.cache.remote.netty.message;

import com.google.common.primitives.Booleans;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RemoteReadFinishResponse extends RPCMessage {
  boolean succeed;

  public RemoteReadFinishResponse(boolean succeed, long msgId) {
    super(msgId);
    this.succeed = succeed;
  }

  @Override
  public Type getType() {
    return Type.REMOTE_READ_RESPONSE;
  }

  @Override
  public void encode(ByteBuf out) {
    out.writeBoolean(succeed);
    encodeMessageId(out);
  }

  @Override
  public int getEncodedLength() {
    return Integer.BYTES + getMessageIdEncodedlength();
  }

  public static RemoteReadFinishResponse decode(ByteBuf in) throws IOException {
    boolean succeed = in.readBoolean();
    long messageId = decodeMessageId(in);
    return new RemoteReadFinishResponse(succeed, messageId);
  }
}
