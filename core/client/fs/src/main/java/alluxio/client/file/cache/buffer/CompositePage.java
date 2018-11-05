package alluxio.client.file.cache.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledDirectByteBuf;

import java.util.List;

public class CompositePage extends UnpooledDirectByteBuf {
  private List<ByteBuf> mInnerPages;

  public CompositePage(List<ByteBuf> l) {
    super(null, 0, 0);
    mInnerPages = l;
  }

  public List<ByteBuf> getBuffer() {
    return mInnerPages;
  }
}
