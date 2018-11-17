package alluxio.client.file.cache.buffer;


import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ByteBufferOutputStream extends OutputStream {
  protected final ByteBuf _b;

  public ByteBufferOutputStream(ByteBuf buf) {
    this._b = buf;
  }

  public void write(int b) throws IOException {
    this._b.writeByte((byte)b);
  }

  public void write(byte[] bytes, int off, int len) throws IOException {
    this._b.writeBytes(bytes, off, len);
  }
}