package alluxio.client.file.cache.remote.stream;

import alluxio.client.file.FileInStream;
import io.netty.buffer.ByteBuf;

import java.io.IOException;

public class FakeFileInStream extends FileInStream {
  protected int mPos;
  public long getPos() {
    return mPos;
  }

  public int read(byte[] b, int off, int len) throws IOException {

    for (int i = off; i < len; i ++) {
      b[i] = (byte)i;
    }
    mPos += len;
    return len;
  }

  public int read() throws IOException {
    mPos ++;
    return mPos;
  }


  @Override
  public long skip(long n) throws IOException {
    mPos += n;
    return n;
  }


}
