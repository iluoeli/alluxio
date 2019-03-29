package alluxio.client.file.cache.remote;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MappedCacheEntity extends FileCacheEntity {
  private int mMappedIndex;
  private int mCurrentMappedLength;

  public MappedCacheEntity(long fileId, String filePath, long fileLength) {
    super(fileId , filePath, fileLength);
    mMappedIndex = -1;
    mCurrentMappedLength = 0;
  }

  public ByteBuf getBuffer(int index) throws IOException {
    if (index > mMappedIndex) {
      for (int i = mMappedIndex + 1; i <= index; i++) {
        int buflength = Math.min(bufferLength, (int)mFileLength - mCurrentMappedLength);
        MappedByteBuffer buffer = mChannel.map(FileChannel.MapMode.READ_ONLY, mCurrentMappedLength, buflength);
        mData.add(Unpooled.wrappedBuffer(buffer));
        mCurrentMappedLength += bufferLength;
        mMappedIndex ++;
      }
    }
    return mData.get(index);
  }
}
