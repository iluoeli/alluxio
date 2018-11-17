package alluxio.client.file.cache.test;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.cache.ClientCacheContext;
import alluxio.client.file.cache.buffer.MemoryAllocator;
import alluxio.client.file.cache.remote.CacheClient;
import alluxio.client.file.cache.remote.CacheServer;
import alluxio.client.file.cache.testRead;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.RandomUtils;

import java.io.IOException;
import java.util.List;

public class CacheClientServerTest extends testRead {
  private CacheServer server;
  private ClientCacheContext mContext = ClientCacheContext.INSTANCE;
  private long fileId;

  public void initServer(AlluxioURI uri) throws Exception {
    FileSystem fs = FileSystem.Factory.get(true);
    fs.openFile(uri);
    fileId = mContext.getMetedataCache().getStatus(uri).getFileId();
    mContext.cache(uri, 0, 1024 * 1024);
    server = new CacheServer(26666, mContext);
  }

  public void initClient() throws Exception {
    MemoryAllocator allocator = new MemoryAllocator();
    CacheClient client = new CacheClient("localhost", 26666, allocator);
    List<ByteBuf> l =  client.getDataFromRemote(fileId, 0,  1024 * 1024);
  }

  public static void main(String [] s) throws Exception {
    //AlluxioURI uri = writeToAlluxio("/usr/local/test.gz", "/testWriteBig");
    AlluxioURI uri = new AlluxioURI("/testWriteBig");
    CacheClientServerTest test = new CacheClientServerTest();
    test.initServer(uri);
    test.initClient();
  }
}
