package alluxio.client.file.cache.remote;

import alluxio.client.file.cache.CacheInternalUnit;
import alluxio.client.file.cache.Metric.HitRatioMetric;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;

import java.util.Iterator;
import java.util.List;

public class FileCacheEntity extends CacheInternalUnit {
  private long mFileLength;


  public FileCacheEntity(long  fileId, long fileLength) {
    super(0, fileLength , fileId);
    mFileLength = fileLength;
  }

  public FileCacheEntity(long fileId, long fileLength, List<ByteBuf> data) {
    super(0, fileLength, fileId, data);
    mFileLength = fileLength;

  }

  public long getFileLength() {
    return mFileLength;
  }



}
