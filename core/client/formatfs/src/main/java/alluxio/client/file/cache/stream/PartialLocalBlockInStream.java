package alluxio.client.file.cache.stream;

import alluxio.Seekable;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.file.cache.ClientCacheContext;
import alluxio.client.file.cache.MetedataCache;
import alluxio.exception.PreconditionMessage;
import alluxio.util.io.BufferUtils;
import alluxio.wire.BlockInfo;
import com.google.common.base.Preconditions;
import io.netty.util.concurrent.CompleteFuture;
import org.apache.avro.file.SeekableByteArrayInput;
import org.jboss.netty.util.internal.ByteBufferUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class PartialLocalBlockInStream extends InputStream {
  private List<ByteBuffer> mReadedBuffer = new ArrayList<>();
  private FileChannel mChannel;
  private ClientCacheContext mContext = ClientCacheContext.INSTANCE;
  int mPacketLength = 1024 * 32;
  long mPosition;
  long mLength;
  ByteBuffer mCurrentBuffer;
  int mCurrentBufferReadedLength;
  long mBlockId;

  public PartialLocalBlockInStream(BlockInfo info) {
    long blockId=  info.getBlockId();
    mBlockId = blockId;
    String path = ClientCacheContext.INSTANCE.getMetedataCache().getPath(blockId);
    if (path == null) {
     path = acquireBlockPath();
    }
    try {
      RandomAccessFile randomAccessFile = new RandomAccessFile(path, "r");
      mChannel = randomAccessFile.getChannel();
      mLength = mChannel.size();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    mPosition = 0;
  }

  public ByteBuffer getCurrent(int length) throws IOException {
    return mChannel.map(FileChannel.MapMode.READ_ONLY, mPosition, length);
  }

  public void releaseBuffer(ByteBuffer byteBuffer) {
    mReadedBuffer.add(mCurrentBuffer);
  }


  @Override
  public void close() throws IOException {
    CompletableFuture<Boolean> closeFuture = CompletableFuture.supplyAsync(
            ()-> {
              for (ByteBuffer buffer : mReadedBuffer) {
                BufferUtils.cleanDirectBuffer(buffer);
              }
              return true;
            }, mContext.COMPUTE_POOL);
    mChannel.close();
  }


  @Override
  public long skip(long n) {
    if (mCurrentBuffer != null) {
      long remaining = mCurrentBuffer.capacity() - mCurrentBufferReadedLength;
      if (n >= remaining) {
        mCurrentBufferReadedLength = 0;
        releaseBuffer(mCurrentBuffer);
        n -= remaining;
      } else {
        mCurrentBufferReadedLength += n;
        return n;
      }
    }

    if (mPosition + n <= mLength) {
      mPosition += n;
      return n;
    } else {
      return mLength - mPosition;
    }
  }


  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int readLength = Math.min(len, (int)(mLength - mPosition));
    while (readLength > 0) {
      int packetLength = mPacketLength;
      if (mCurrentBuffer == null) {
        mCurrentBuffer = getCurrent(Math.min(mPacketLength, readLength));
      } else {
        packetLength = mCurrentBuffer.capacity() - mCurrentBufferReadedLength;
      }

      int currPacketLen = Math.min(packetLength, readLength);

      mCurrentBuffer.get(b, off, currPacketLen);
      if (packetLength > readLength) {
        mCurrentBufferReadedLength += readLength;
      } else {
        releaseBuffer(mCurrentBuffer);
        mCurrentBuffer = null;
        mCurrentBufferReadedLength = 0;
      }
      off += currPacketLen;
      mPosition += currPacketLen;
      readLength -= currPacketLen;
    }
    return readLength == 0? -1 : readLength;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read() throws IOException {
    if (mPosition == mLength) {
      return -1;
    }
    if (mCurrentBuffer == null) {
      mCurrentBuffer = getCurrent((int)Math.min(mPacketLength, mLength - mPosition));
      mCurrentBufferReadedLength = 0;
    }

    int res = mCurrentBuffer.get(mCurrentBufferReadedLength);
    mCurrentBufferReadedLength ++;
    mPosition ++;
    if (mCurrentBufferReadedLength == mCurrentBuffer.capacity()) {
      mCurrentBufferReadedLength = 0;
      releaseBuffer(mCurrentBuffer);
      mCurrentBuffer = null;
    }
    return res;
  }

  public String acquireBlockPath() {
    //todo
    return null;
  }

}
