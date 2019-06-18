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

import alluxio.client.file.cache.submodularLib.Element;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.List;

public interface CacheUnit extends Element {

  public boolean isFinish();

  public long getBegin();

  public long getEnd();

  public long getFileId();

  public long getSize();

  public double getHitValue();

  public void setCurrentHitVal(double hit);

  public void setLockTask(LockTask task);

  public LockTask getLockTask();

  public List<ByteBuf> get(long pos, long len) throws IOException;
}
