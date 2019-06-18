package alluxio.client.file.cache.remote;

import alluxio.client.file.cache.core.CacheInternalUnit;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.List;

public class FileCacheEntity extends CacheInternalUnit {
  long mFileLength;
  FileChannel mChannel;
  int bufferLength = FileCacheContext.LOCAL_BUFFER_SIZE;

  public FileCacheEntity(long  fileId, long fileLength) {
    super(0, fileLength , fileId);
    mFileLength = fileLength;
  }

  public FileCacheEntity(long fileId, long fileLength, List<ByteBuf> data) {
    super(0, fileLength, fileId, data);
    mFileLength = fileLength;
  }

  public FileCacheEntity(long fileId, String filePath, long fileLength) {
    super(0, fileLength , fileId);
    mFileLength = fileLength;
    try {
      RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "r");
      mChannel = randomAccessFile.getChannel();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public long getFileLength() {
    return mFileLength;
  }

  public ByteBuf getBuffer(int index) throws IOException {
    int pos = index * bufferLength;
    int buffflength = Math.min(bufferLength, (int)mFileLength - pos);
    return Unpooled.wrappedBuffer(mChannel.map(FileChannel.MapMode.READ_ONLY, pos, buffflength));
  }
}
