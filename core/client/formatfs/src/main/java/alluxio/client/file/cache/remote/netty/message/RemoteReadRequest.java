package alluxio.client.file.cache.remote.netty.message;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.SerializationUtils;

import java.util.HashMap;

public class RemoteReadRequest extends RPCMessage {
  private long mFileId;
  private long mBegin;
  private long mEnd;

  public RemoteReadRequest(long fileId, long begin, long end) {
    mFileId = fileId;
    mBegin = begin;
    mEnd = end;
  }

  public RemoteReadRequest(long fileId, long begin, long end, long messageId) {
    super(messageId);
    mFileId = fileId;
    mBegin = begin;
    mEnd = end;
  }

  public long getFileId() {
    return mFileId;
  }

  public long getBegin() {
    return mBegin;
  }

  public long getEnd() {
    return mEnd;
  }

  @Override
  public void encode(ByteBuf out) {
    encodeMessageId(out);
    out.writeLong(mFileId);
    out.writeLong(mBegin);
    out.writeLong(mEnd);
  }

  public static RemoteReadRequest decode(ByteBuf in) {
    long messageId = decodeMessageId(in);
    return  new RemoteReadRequest(in.readLong(), in.readLong(), in
      .readLong(), messageId);
    }


  @Override
  public int getEncodedLength() {
    return Long.BYTES * 3 + getMessageIdEncodedlength();
  }

  @Override
  public Type getType() {
    return Type.REMOTE_READ_REQUEST;
  }
}
