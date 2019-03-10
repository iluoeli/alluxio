package alluxio.client.file.cache.remote.netty.message;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class RemoteReadResponse extends RPCMessage implements PayloadMessage {
  private List<ByteBuf> mData;
  private int mLength;

  public RemoteReadResponse(long messageId, List<ByteBuf> data, int length) {
    super(messageId);
    mData = data;
    mLength = length;
  }

  @Override
  public Type getType() {
    return Type.REMOTE_READ_RESPONSE;
  }

  @Override
  public void encode(ByteBuf out) {
    out.writeInt(mLength);
    encodeMessageId(out);
  }

  @Override
  public int getEncodedLength() {
    return Integer.BYTES + mLength + getMessageIdEncodedlength() ;
  }

  public static RemoteReadResponse decode(ByteBuf in) throws IOException {
    int length = in.readInt();
    long messageId = decodeMessageId(in);
    ByteBuf data = decodeData(in, length);
    List<ByteBuf> l = new ArrayList<>();
    l.add(data);
    return new RemoteReadResponse(messageId, l, length);
  }

  @Override
  public List<ByteBuf> getPayload() {
    return mData;
  }

  // todo need release the bytebuf after the client reads finish.
  private static ByteBuf decodeData(ByteBuf in, int length) {
    in.retain();
    return  in.slice(0, length);
  }
}
