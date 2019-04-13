package alluxio.client.file.cache.stream;

import alluxio.Constants;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.ClientCacheContext;
import alluxio.collections.Pair;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.network.TieredIdentityFactory;
import alluxio.resource.CloseableResource;
import alluxio.retry.CountingRetry;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerNetAddress;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.util.*;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class PartialFileInStream extends FileInStream  {

  private LinkedList<InputStream> mBlockInputStreams = new LinkedList<>();
  private ClientCacheContext mContext = ClientCacheContext.INSTANCE;
  private FileSystem fs;
  static TieredIdentity mTieredIdentity = TieredIdentityFactory.localIdentity();
  private long mLength;
  private long mPosition;

  public PartialFileInStream(long fileId) {
    try {
      URIStatus fileStatus = mContext.getMetedataCache().getStatus(fileId);
      if (fileStatus == null) {
        System.out.println("file metedata cache missed ===============");
        fs = FileSystem.Factory.get();
        fs.getStatus(mContext.getMetedataCache().getUri(fileId));
      }
      Preconditions.checkNotNull(fileStatus);
      mPosition = 0;
      mLength = fileStatus.getLength();
      for (long blockId : fileStatus.getBlockIds()) {
        if (fs == null) {
          fs = FileSystem.Factory.get();
        }
        BlockInfo info;
        try (CloseableResource<BlockMasterClient> masterClientResource =
                     FileSystemContext.get().acquireBlockMasterClientResource()) {
          info = masterClientResource.get().getBlockInfo(blockId);
        }
        if (info == null) {
          throw new NullPointerException();
        }
        Pair<WorkerNetAddress, BlockInStream.BlockInStreamSource> locationInfo =
                handleBlockInfo(info);
        Preconditions.checkNotNull(locationInfo);
        if (locationInfo.getSecond() == BlockInStream.BlockInStreamSource.LOCAL) {
          mBlockInputStreams.addLast(new PartialLocalBlockInStream(info));
        } else {
          mBlockInputStreams.addLast(new PartialRemoteBlockInStream(locationInfo.getFirst(), info.getBlockId()));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  /* Input Stream methods */
  @Override
  public int read() throws IOException {
    if (mPosition == mLength) { // at end of file
      return -1;
    }
    //todo
    return -1;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }


  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return -1;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    long toSkip = Math.min(n, mLength - mPosition);
    seek(mPosition + toSkip);
    return toSkip;
  }

  @Override
  public void close() throws IOException {

  }

  /* Bounded Stream methods */
  @Override
  public long remaining() {
    return mLength - mPosition;
  }

  /* Positioned Readable methods */
  @Override
  public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
     throw new RuntimeException("unSupport operation");
  }


  /* Seekable methods */
  @Override
  public long getPos() {
    return mPosition;
  }

  public static Pair<WorkerNetAddress, BlockInStream.BlockInStreamSource > handleBlockInfo(BlockInfo info) throws Exception {
    Set<WorkerNetAddress> workerPool;
    List<BlockLocation> locations = info.getLocations();


      workerPool = locations.stream().map(BlockLocation::getWorkerAddress).collect(toSet());

    if (workerPool.isEmpty()) {
      throw new NotFoundException(ExceptionMessage.BLOCK_UNAVAILABLE.getMessage(info.getBlockId()));
    }
    // Workers to read the block, after considering failed workers.
    Set<WorkerNetAddress> workers = workerPool;
            //handleFailedWorkers(workerPool, failedWorkers);
    // TODO(calvin): Consider containing these two variables in one object
    BlockInStream.BlockInStreamSource dataSourceType = null;
    WorkerNetAddress dataSource = null;
    locations = locations.stream()
            .filter(location -> workers.contains(location.getWorkerAddress())).collect(toList());
    // First try to read data from Alluxio
    if (!locations.isEmpty()) {
      // TODO(calvin): Get location via a policy
      List<TieredIdentity> tieredLocations =
              locations.stream().map(location -> location.getWorkerAddress().getTieredIdentity())
                      .collect(toList());
      Collections.shuffle(tieredLocations);
      Optional<TieredIdentity> nearest = mTieredIdentity.nearest(tieredLocations);
      if (nearest.isPresent()) {
        dataSource = locations.stream().map(BlockLocation::getWorkerAddress)
                .filter(addr -> addr.getTieredIdentity().equals(nearest.get())).findFirst().get();
        if (mTieredIdentity.getTier(0).getTierName().equals(Constants.LOCALITY_NODE)
                && mTieredIdentity.topTiersMatch(nearest.get())) {
          dataSourceType = BlockInStream.BlockInStreamSource.LOCAL;
        } else {
          dataSourceType = BlockInStream.BlockInStreamSource.REMOTE;
        }
        return new Pair<>(dataSource, dataSourceType);
      }
    }
    return null;
  }
}
