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

public class StatvfsTest {
  @Test
  public void offset() {
    Statvfs jni = new Statvfs(ByteBuffer.allocate(256));
    ru.serce.jnrfuse.struct.Statvfs jnr =
        ru.serce.jnrfuse.struct.Statvfs.of(Pointer.wrap(Runtime.getSystemRuntime(), 0x0));
    assertEquals(jnr.f_bsize.offset(), jni.f_bsize.offset());
    assertEquals(jnr.f_frsize.offset(), jni.f_frsize.offset());
    assertEquals(jnr.f_blocks.offset(), jni.f_blocks.offset());
    assertEquals(jnr.f_bfree.offset(), jni.f_bfree.offset());
    assertEquals(jnr.f_bavail.offset(), jni.f_bavail.offset());
    assertEquals(jnr.f_files.offset(), jni.f_files.offset());
    assertEquals(jnr.f_ffree.offset(), jni.f_ffree.offset());
    assertEquals(jnr.f_favail.offset(), jni.f_favail.offset());
    assertEquals(jnr.f_fsid.offset(), jni.f_fsid.offset());
    assertEquals(jnr.f_flag.offset(), jni.f_flag.offset());
    assertEquals(jnr.f_namemax.offset(), jni.f_namemax.offset());
  }

  @Test
  public void dataConsistency() {
    Statvfs statvfs = new Statvfs(ByteBuffer.wrap(new byte[128]));
    int bsize = 0x1234;
    int frsize = 0x1335;
    int blocks = 0x1436;
    int bfree = 0x1537;
    int bavail = 0x1638;
    int files = 0x1739;
    int ffree = 0x183a;
    int favail = 0x193b;
    int fsid = 0x1a3c;
    int flag = 0x1b3d;
    int namemax = 0x1c3e;
    statvfs.f_bsize.set(bsize);
    statvfs.f_frsize.set(frsize);
    statvfs.f_blocks.set(blocks);
    statvfs.f_bfree.set(bfree);
    statvfs.f_bavail.set(bavail);
    statvfs.f_files.set(files);
    statvfs.f_ffree.set(ffree);
    statvfs.f_favail.set(favail);
    statvfs.f_fsid.set(fsid);
    statvfs.f_flag.set(flag);
    statvfs.f_namemax.set(namemax);
    assertEquals(bsize, statvfs.f_bsize.get());
    assertEquals(frsize, statvfs.f_frsize.get());
    assertEquals(blocks, statvfs.f_blocks.get());
    assertEquals(bfree, statvfs.f_bfree.get());
    assertEquals(bavail, statvfs.f_bavail.get());
    assertEquals(files, statvfs.f_files.get());
    assertEquals(ffree, statvfs.f_ffree.get());
    assertEquals(favail, statvfs.f_favail.get());
    assertEquals(fsid, statvfs.f_fsid.get());
    assertEquals(flag, statvfs.f_flag.get());
    assertEquals(namemax, statvfs.f_namemax.get());
  }
}
