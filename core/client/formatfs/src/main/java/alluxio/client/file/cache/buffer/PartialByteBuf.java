package alluxio.client.file.cache.buffer;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledDirectByteBuf;


public class PartialByteBuf extends UnpooledDirectByteBuf {
	protected ByteBuf mBuffer;
  protected int mLength;

  public PartialByteBuf(ByteBuf buffer, int length) {
    super(null, 0, 0);
    mBuffer = buffer;
    mLength = length;
  }

  public PartialByteBuf updateLength(int length) {
    mLength = length;
    return this;
  }

  public int capacity() {
    return mLength;
  }

  public byte getByte(int i) {
    return mBuffer.getByte(i);
  }

	public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
		mBuffer.getBytes(index, dst, dstIndex, length);
		return this;
	}

	public ByteBuf readerIndex(int i) {
    return mBuffer.readerIndex(i);
  }

	public ByteBuf writeBytes(byte[] src, int srcIndex, int length) {
  	mBuffer.writeBytes(src, srcIndex, length);
  	return this;
	}


	@Override
  public ByteBuf slice(int a, int b) {
    Preconditions.checkArgument( a + b < mLength);
    mBuffer = mBuffer.slice(a, b);
    mLength = b;
    return this;
  }

  @Override
  public ByteBuf slice() {
		mLength = mLength - mBuffer.readerIndex();
		mBuffer = mBuffer.slice();
		return this;
  }

  public ByteBuf getBuffer() {
  	return mBuffer;
	}
}
