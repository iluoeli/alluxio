package alluxio.client.file.cache.remote;

import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.ClientCacheContext;
import alluxio.client.file.options.InStreamOptions;

import static alluxio.client.file.cache.ClientCacheContext.fileId;

public class RemoteFileInStream extends FileInStream {
  public CacheManager mCachePolicy;
  long mPosition;
  final long mLength;
  final long mFileId;
  ClientCacheContext.LockManager mLockManager;

  public RemoteFileInStream(InStreamOptions opt, ClientCacheContext context, URIStatus status) {
    super(status, opt, FileSystemContext.get());
    mPosition = 0;
    mLength = status.getLength();
    mFileId = status.getFileId();
    fileId = mFileId;
  }



}
