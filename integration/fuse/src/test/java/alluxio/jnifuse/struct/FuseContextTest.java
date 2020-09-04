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
    FuseContext jnifc = new FuseContext(ByteBuffer.allocate(128));
    ru.serce.jnrfuse.struct.FuseContext jnrfc =
        ru.serce.jnrfuse.struct.FuseContext.of(Pointer.wrap(Runtime.getSystemRuntime(), 0x0));
    assertEquals(jnrfc.fuse.offset(), jnifc.fuse.offset());
    assertEquals(jnrfc.uid.offset(), jnifc.uid.offset());
    assertEquals(jnrfc.gid.offset(), jnifc.gid.offset());
    assertEquals(jnrfc.pid.offset(), jnifc.pid.offset());
    assertEquals(jnrfc.private_data.offset(), jnifc.private_data.offset());
    assertEquals(jnrfc.umask.offset(), jnifc.umask.offset());
  }

  @Test
  public void dataConsistency() {
    FuseContext fi = new FuseContext(ByteBuffer.wrap(new byte[128]));
    int fuse=0x7fffffff;
    int uid = 1000;
    int gid = 1000;
    int pid = 32728;
    int private_date = 0x7fffffff;
    int umask = 0x1234;
    fi.fuse.set(fuse);
    fi.uid.set(uid);
    fi.gid.set(gid);
    fi.pid.set(pid);
    fi.private_data.set(private_date);
    fi.umask.set(umask);
    assertEquals(fuse, fi.fuse.get());
    assertEquals(uid, fi.uid.get());
    assertEquals(gid, fi.gid.get());
    assertEquals(pid, fi.pid.get());
    assertEquals(private_date, fi.private_data.get());
    assertEquals(umask, fi.umask.get());
  }
}
