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

import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class FuseContextTest {
  @Test
  public void offset() {
    FuseContext jnictx = new FuseContext(ByteBuffer.allocate(256));
    ru.serce.jnrfuse.struct.FuseContext jnrctx =
        ru.serce.jnrfuse.struct.FuseContext.of(Pointer.wrap(Runtime.getSystemRuntime(), 0x0));
    assertEquals(jnrctx.fuse.offset(), jnictx.fuse.offset());
    assertEquals(jnrctx.uid.offset(), jnictx.uid.offset());
    assertEquals(jnrctx.gid.offset(), jnictx.gid.offset());
    assertEquals(jnrctx.pid.offset(), jnictx.pid.offset());
    assertEquals(jnrctx.private_data.offset(), jnictx.private_data.offset());
    // assertEquals(jnrctx.umask.offset(), jnictx.umask.offset());
  }
}
