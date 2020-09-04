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

package alluxio.jnifuse.struct;

import java.nio.ByteBuffer;

// TODO(iluoeli): Add more fields
public class FuseContext extends Struct {

  public final UnsignedLong fuse = new UnsignedLong();
  public final uid_t uid = new uid_t();
  public final gid_t gid = new gid_t();
  public final pid_t pid = new pid_t();
  public final UnsignedLong private_data = new UnsignedLong();
  public final mode_t umask = new mode_t();

  public FuseContext(ByteBuffer buffer) {
    super(buffer);
  }

  public FuseContext warp(ByteBuffer buffer) {
    return new FuseContext(buffer);
  }
}
