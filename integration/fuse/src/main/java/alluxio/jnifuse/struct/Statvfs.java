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

public class Statvfs extends Struct {
  /* Definitions for the flag in `f_flag'. */
  public static final int ST_RDONLY = 1; /* Mount read-only. */
  public static final int ST_NOSUID = 2; /* Ignore suid and sgid bits. */
  public static final int ST_NODEV = 4; /* Disallow access to device special files. */
  public static final int ST_NOEXEC = 8; /* Disallow program execution. */
  public static final int ST_SYNCHRONOUS = 16;/* Writes are synced at once. */
  public static final int ST_MANDLOCK = 64; /* Allow mandatory locks on an FS. */
  public static final int ST_WRITE = 128; /* Write on file/directory/symlink. */
  public static final int ST_APPEND = 256; /* Append-only file. */
  public static final int ST_IMMUTABLE = 512; /* Immutable file. */
  public static final int ST_NOATIME = 1024; /* Do not update access times. */
  public static final int ST_NODIRATIME = 2048;/* Do not update directory access times. */
  public static final int ST_RELATIME = 4096; /* Update atime relative to mtime/ctime. */

  public Statvfs(ByteBuffer buffer) {
    super(buffer);
    f_bsize = new UnsignedLong();
    f_frsize = new UnsignedLong();
    f_blocks = new fsblkcnt_t();
    f_bfree = new fsblkcnt_t();
    f_bavail = new fsblkcnt_t();
    f_files = new fsblkcnt_t();
    f_ffree = new fsblkcnt_t();
    f_favail = new fsblkcnt_t();
    f_fsid = new UnsignedLong();
    f_unused = null;
    f_flag = new UnsignedLong();
    f_namemax = new UnsignedLong();
  }

  public final UnsignedLong f_bsize;
  public final UnsignedLong f_frsize;
  public final fsblkcnt_t f_blocks;
  public final fsblkcnt_t f_bfree;
  public final fsblkcnt_t f_bavail;
  public final fsblkcnt_t f_files;
  public final fsblkcnt_t f_ffree;
  public final fsblkcnt_t f_favail;
  public final UnsignedLong f_fsid;
  public final u_int32_t f_unused;
  public final UnsignedLong f_flag;
  public final UnsignedLong f_namemax;
  // __f_spare

  public static Statvfs wrap(ByteBuffer buffer) {
    return new Statvfs(buffer);
  }
}
