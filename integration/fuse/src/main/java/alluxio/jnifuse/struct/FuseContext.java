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

public class FuseContext extends Struct {

  public final Unsigned64 fuse = new Unsigned64();
  public final Unsigned32 uid = new Unsigned32();
  public final Unsigned32 gid = new Unsigned32();
  public final Signed32 pid = new Signed32();
  public final Unsigned64 private_data = new Unsigned64();
  public final Unsigned16 umask = new Unsigned16();

  public FuseContext(ByteBuffer buffer) {
    super(buffer);
  }

  public static FuseContext wrap(ByteBuffer buf) {
    return new FuseContext(buf);
  }
}
