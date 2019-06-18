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

package alluxio.client.file.cache.core;

import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;

import java.io.IOException;

public class FileOutStreamWithCache extends FileOutStream {
  protected final ClientCacheContext mCacheContext;

  public FileOutStreamWithCache(AlluxioURI path, OutStreamOptions options, FileSystemContext context, ClientCacheContext cacheContext) throws IOException {
    super(path, options, context);
    mCacheContext = cacheContext;
  }
}
