package alluxio.client.file.cache.remote.netty.message;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.List;

/**
 * Represents some type of messages that have appending data to write via netty besides the message
 * header.
 */
public interface PayloadMessage {

  List<ByteBuf> getPayload() throws IOException;
}
