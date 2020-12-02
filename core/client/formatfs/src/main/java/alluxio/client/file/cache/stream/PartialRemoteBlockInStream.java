package alluxio.client.file.cache.stream;

import alluxio.client.block.stream.NettyPacketReader;
import alluxio.client.block.stream.PacketReader;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.cache.remote.netty.message.RemoteReadResponse;
import alluxio.exception.status.UnavailableException;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.proto.status.Status;
import alluxio.util.CommonUtils;
import alluxio.util.network.NettyUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.wire.WorkerNetAddress;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;


import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.PriorityQueue;

public class PartialRemoteBlockInStream extends InputStream {
  int mPacketLength = 1024 * 1024 * 8;
  private WorkerNetAddress mAddress;
  private Channel mChannel = null;
  private static final ByteBuf EOF_OR_CANCELLED = Unpooled.buffer(0);
  private static final ByteBuf THROWABLE = Unpooled.buffer(0);
  private Protocol.ReadRequest.Builder mBuilder;
  private PriorityQueue<PacketReader> mReceiveData = new PriorityQueue<>(new Comparator<PacketReader>() {
    @Override
    public int compare(PacketReader o1, PacketReader o2) {
      return (int)(o1.pos() - o2.pos());
    }
  });
  private long mPosition;
  private long mLength;
  private long mBlockId;

  private DataBuffer mBuffer = null;

  public PartialRemoteBlockInStream(WorkerNetAddress address, long blockId) {
    mBlockId = blockId;
    mAddress = address;
    mBuilder = Protocol.ReadRequest.newBuilder().setBlockId(mBlockId).setPromote(false);
  }

  public void addReadPacket(long pos, long length) throws IOException {
    PacketReader.Factory factory = new NettyPacketReader.Factory(FileSystemContext.get(),
            mAddress, mBuilder.setPacketSize(mPacketLength).buildPartial());
    mReceiveData.add(factory.create(pos, length));
  }
  @Override
  public int read() throws IOException {
    //todo
    return -1;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int readLength = Math.min(len, (int) (mLength - mPosition));
    while (readLength > 0) {
      if (!mReceiveData.isEmpty() && (mReceiveData.peek() == EOF_OR_CANCELLED ||
              mReceiveData.peek() == THROWABLE)) {
        mReceiveData.poll();
        continue;
      } else if (mReceiveData.isEmpty() || mReceiveData.peek().pos() > mPosition) {
        continue;
      }

      PacketReader reader = mReceiveData.peek();
      boolean isLast = mBuffer != null;
      DataBuffer buffer = mBuffer == null ? reader.readPacket() : mBuffer;
      long currPos = reader.pos() - mBuffer.readableBytes();

      while (currPos + buffer.readableBytes() < mPosition) {
        if (isLast) {
          mBuffer = null;
        }
        currPos += buffer.readableBytes();
        buffer = reader.readPacket();
      }

      while (currPos < mPosition) {
        buffer.getReadOnlyByteBuffer().get();
        currPos ++;
      }

      long bufferRemain = buffer.readableBytes();
      int readLen = Math.min(readLength, buffer.readableBytes());
      buffer.readBytes(b, off, readLen);
      mPosition += readLen;
      off -= readLen;
      len -= readLen;
      readLength -= readLen;
      if (bufferRemain <= readLen) {
        mBuffer = null;
      } else {
        mBuffer = buffer;
        return readLen == 0? -1 : readLen;
      }
    }
    return readLength == 0 ? -1 :readLength;
  }
}
