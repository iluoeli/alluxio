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
		return null;
	}

	@Override
	public ByteBuf slice() {
		return null;
	}

	@Override
	public boolean release() {
		return mBuffer.release();
	}


}
