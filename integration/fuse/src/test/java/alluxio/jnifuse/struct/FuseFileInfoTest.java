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

import static org.junit.Assert.assertEquals;

import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.junit.Test;

import java.nio.ByteBuffer;

public class FuseFileInfoTest {
  @Test
  public void offset() {
    FuseFileInfo jnifi = new FuseFileInfo(ByteBuffer.allocate(128));
    ru.serce.jnrfuse.struct.FuseFileInfo jnrfi =
        ru.serce.jnrfuse.struct.FuseFileInfo.of(Pointer.wrap(Runtime.getSystemRuntime(), 0x0));
    assertEquals(jnrfi.flags.offset(), jnifi.flags.offset());
    assertEquals(jnrfi.fh_old.offset(), jnifi.fh_old.offset());
    assertEquals(jnrfi.fh.offset(), jnifi.fh.offset());
    assertEquals(jnrfi.lock_owner.offset(), jnifi.lock_owner.offset());
  }

  @Test
  public void dataConsistency() {
    FuseFileInfo fi = new FuseFileInfo(ByteBuffer.wrap(new byte[128]));
    int flags = 0x12345678;
    int fh_old = 0x3456789a;
    int fh = 0x56789abc;
    int lock_owner = 0x789abcde;
    fi.flags.set(flags);
    fi.fh_old.set(fh_old);
    fi.fh.set(fh);
    fi.lock_owner.set(lock_owner);
    assertEquals(flags, fi.flags.get());
    assertEquals(fh_old, fi.fh_old.get());
    assertEquals(fh, fi.fh.get());
    assertEquals(lock_owner, fi.lock_owner.get());
  }
}
