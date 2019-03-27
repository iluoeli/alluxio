package alluxio.client.file.cache.remote.netty.message;

import com.google.common.primitives.Booleans;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RemoteReadFinishResponse extends RPCMessage {
  long mFileLength;

  public RemoteReadFinishResponse(long msgId, long fileLength) {
    super(msgId);
    mFileLength = fileLength;
  }

  public long getFileLength() {
    return mFileLength;
  }

  @Override
  public Type getType() {
    return Type.REMOTE_READ_FINISH_RESPONSE;
  }

  @Override
  public void encode(ByteBuf out) {
    out.writeLong(mFileLength);
    encodeMessageId(out);
  }

  @Override
  public int getEncodedLength() {
    return Long.BYTES + getMessageIdEncodedlength();
  }

  public static RemoteReadFinishResponse decode(ByteBuf in) throws IOException {
    long fileLength = in.readLong();
    long messageId = decodeMessageId(in);
    return new RemoteReadFinishResponse(messageId, fileLength);
  }

  public String toString() {
    return "finish : " + " length " + mFileLength;
  }
}
