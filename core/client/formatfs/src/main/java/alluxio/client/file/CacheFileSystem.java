/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.client.file.cache.core.ClientCacheContext;
import alluxio.client.file.cache.core.FileInStreamWithCache;
import alluxio.client.file.cache.core.MetedataCache;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;

import java.io.IOException;

import static alluxio.client.file.cache.core.ClientCacheContext.useMetedata;

public class CacheFileSystem extends BaseFileSystem {

  private final ClientCacheContext mCacheContext;
  private static MetedataCache metedataCache;

  public static CacheFileSystem get(FileSystemContext context, ClientCacheContext cacheContext) {
    metedataCache = cacheContext.getMetedataCache();
    return new CacheFileSystem(context, cacheContext);
  }

  public static FileSystem get(boolean isCache) {
    if (isCache) {
      return get(FileSystemContext.get(), ClientCacheContext.INSTANCE);
    } else {
      return  FileSystem.Factory.get(FileSystemContext.get());
    }
  }

  private CacheFileSystem(FileSystemContext context, ClientCacheContext cacheContext) {
    super(context);
    mCacheContext = cacheContext;
  }

  @Override
  public FileInStream openFile(AlluxioURI path) throws IOException, AlluxioException {
    if (!useMetedata)
      return openFile(path, OpenFileOptions.defaults());
    else
      return openFileWithCache(path);
  }

  @Override
  public FileInStream openFile(AlluxioURI path, OpenFileOptions options) throws IOException, AlluxioException {
    URIStatus status = getStatus(path);
    if (status.isFolder()) {
      throw new FileDoesNotExistException(
        ExceptionMessage.CANNOT_READ_DIRECTORY.getMessage(status.getName()));
    }
    InStreamOptions inStreamOptions = options.toInStreamOptions(status);
    FileInStream in = new FileInStream(status, inStreamOptions, mFileSystemContext);
    return new FileInStreamWithCache(inStreamOptions, mCacheContext, status);
  }

  public FileInStream openFileWithCache(AlluxioURI path) throws IOException, AlluxioException {
    URIStatus status;
    if (!metedataCache.cached(path)) {
      status = getStatus(path);
      metedataCache.cacheMetedata(path, status);
    } else {
      status = metedataCache.getStatus(path);
    }
    if (status.isFolder()) {
      throw new FileDoesNotExistException(
        ExceptionMessage.CANNOT_READ_DIRECTORY.getMessage(status.getName()));
    }



    InStreamOptions inStreamOptions = OpenFileOptions.defaults().toInStreamOptions(status);
    return new FileInStreamWithCache(inStreamOptions, mCacheContext, status);
  }
}
