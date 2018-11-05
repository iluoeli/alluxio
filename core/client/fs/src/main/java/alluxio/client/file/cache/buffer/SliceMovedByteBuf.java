package alluxio.client.file.cache.buffer;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;

import java.util.List;

public class SliceMovedByteBuf extends PartialByteBuf {

	public SliceMovedByteBuf(ByteBuf byteBuf, int length) {
		super(byteBuf, length);
	}

	@Override
	public ByteBuf slice(int a, int b) {
		Preconditions.checkArgument( a + b < mLength);
    List<ByteBuf> newPage = PageManager.INSTANCE.getCache(b);
		return this;
	}

	@Override
	public ByteBuf slice() {
		long left = mLength - mBuffer.readerIndex();
		List<ByteBuf> newPage = PageManager.INSTANCE.getCache(left);
    int index = readerIndex();
    int bufIndex = 0;
    ByteBuf curr = newPage.get(bufIndex);
    do {
			curr = newPage.get(bufIndex);

			curr.writeBytes(mBuffer, index, curr.capacity());
			index += curr.capacity();
			bufIndex ++;
		} while(index < mLength && bufIndex < newPage.size());
		release();
		PageManager.INSTANCE.releaseBuffer(this);
		return new CompositePage(newPage);
	}

	@Override
	public boolean release() {
		return mBuffer.release();
	}


}
