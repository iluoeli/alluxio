package alluxio.client.file.cache.remote.stream;

import java.io.IOException;
import java.io.InputStream;

public class UnderFileInputStream extends InputStream {

  private InputStream  mUnderFileInputStream;

  public UnderFileInputStream(InputStream in) {
    mUnderFileInputStream = in;
  }

  public int read(byte[] b, int off, int len) throws IOException {
    return mUnderFileInputStream.read(b, off, len);
  }


  public int read() throws IOException {
    return mUnderFileInputStream.read();
  }

  public void close() throws IOException{
    mUnderFileInputStream.close();
  }


}
