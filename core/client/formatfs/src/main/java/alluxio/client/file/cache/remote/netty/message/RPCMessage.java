package alluxio.client.file.cache.remote.netty.message;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.io.IOException;
import java.util.Random;

public abstract class RPCMessage implements EncodedMessage {


  /**
   * Message id. The response message id should be equal to the request message id if the RPC is
   * synchronous. So the caller can distinguish which request message a received response message
   * corresponds. If the RPC is asynchronous, the id needn't to be set.
   */
  private final long mMessageId;

  /**
   * Default constructor.
   */
  public RPCMessage() {
    mMessageId = Math.abs(new Random(System.currentTimeMillis()).nextLong());
  }

  /**
   * Constructor for {@link RPCMessage}.
   *
   * @param messageId the message id
   */
  public RPCMessage(long messageId) {
    mMessageId = messageId;
  }

  /**
   * Get the message id.
   *
   * @return message id
   */
  public long getMessageId() {
    return mMessageId;
  }

  /**
   * Encodes a message id.
   *
   * @param out the output byte buffer
   */
  public void encodeMessageId(ByteBuf out) {
    out.writeLong(mMessageId);
  }

  /**
   * Decodes message id.
   *
   * @param in the input byte buffer
   * @return the message id
   */
  public static long decodeMessageId(ByteBuf in) {
    return in.readLong();
  }

  /**
   * @return the length of bytes for encoded message id
   */
  public int getMessageIdEncodedlength() {
    return Longs.BYTES;
  }


  /**
   * @return the type of the RPC message
   */
  public abstract Type getType();


  /**
   * Decodes a RPC message from a byte buffer.
   * @param in the input byte buffer
   * @return the corresponding RPC message
   * @throws IOException if failed to decode the message
   */
  public static RPCMessage decodeMessage(ByteBuf in) throws IOException {
    Type type = Type.decode(in);
    return decodeInternal(in, type);
  }

  @Override
  public String toString() {
    return getType().toString();
  }

  /**
   * Internal helper method to decode a RPC message according to its type.
   * @param in the input byte buffer
   * @param type the type of the RPC message
   * @return the corresponding RPC message
   * @throws IOException if failed to decode the message
   */
  private static RPCMessage decodeInternal(ByteBuf in, Type type) throws IOException {
    switch (type) {
      case REMOTE_READ_REQUEST:
        return RemoteReadRequest.decode(in);
      case REMOTE_READ_RESPONSE:
        return RemoteReadResponse.decode(in);
      default:
        throw new IllegalArgumentException("No corresponding RPC message for type " + type);
    }
  }

  /**
   * The types of all actual RPC messages.
   */
  public enum Type implements EncodedMessage {
    REMOTE_READ_REQUEST(0),
    REMOTE_READ_RESPONSE(1),
    REMOTE_READ_FINISH_RESPONSE(2);

    private final int mId;

    Type(int id) {
      mId = id;
    }

    /**
     * @return {@link #mId}
     */
    public int getId() {
      return mId;
    }

    /**
     * Decodes a {@link Type} from an input {@link ByteBuf}.
     *
     * @param in the input byte buffer
     * @return a new type
     */
    public static Type decode(ByteBuf in) {
      int id = (int) in.readShort();
      switch (id) {
        case 0:
          return REMOTE_READ_REQUEST;
        case 1:
          return REMOTE_READ_RESPONSE;
        case 2:
          return REMOTE_READ_FINISH_RESPONSE;
        default:
          throw new IllegalArgumentException("No corresponding RPC message type for " + id);
      }
    }

    @Override
    public void encode(ByteBuf out) {
      out.writeShort((short) mId);
    }

    @Override
    public int getEncodedLength() {
      return Shorts.BYTES;
    }
  }

  public static LengthFieldBasedFrameDecoder createFrameDecoder() {
    return new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, Ints.BYTES, -Ints.BYTES,
      Ints.BYTES);
  }
}
