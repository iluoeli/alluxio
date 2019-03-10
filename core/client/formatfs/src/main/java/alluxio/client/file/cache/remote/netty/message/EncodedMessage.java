package alluxio.client.file.cache.remote.netty.message;

import io.netty.buffer.ByteBuf;

/**
 * This interface represents some type of message that can be encoded into
 * {@link io.netty.buffer.ByteBuf}.
 */
public interface EncodedMessage {
  /**
   * This method encodes the message into a {@link ByteBuf}.
   *
   * @param out the output byte buffer
   */
  void encode(ByteBuf out);

  /**
   * @return the length of bytes after being encoded
   */
  int getEncodedLength();
}
