package alluxio.client.file.cache.buffer;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SharedMemoryAllocator extends MemoryAllocator {

  @Override
  public  List<ByteBuf>  copyIntoCache(ByteString remoteData) {
    List<ByteBuf> allocateRes = null;
    try {
    int left = remoteData.size();
    allocateRes = allocate(left, true);
    int index = 0;
    InputStream in = remoteData.newInput();
    while(in.available() >0 && left > 0) {
      ByteBuf buf = allocateRes.get(index);
      buf.writeBytes(in, buf.capacity());
      left -= buf.capacity();
    }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return allocateRes;
  }


  public List<ByteBuf> allocate(int size, boolean isNew) {
    //TODO add shared memory allocate part
    return null;
  }

  @Override
  public void init() {

  }

  @Override
  public void release(ByteBuf buf) {

  }
}


