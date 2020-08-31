package alluxio.jnifuse.struct;

import jnr.ffi.Runtime;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class TypeAliasTest {
  static class JniStruct extends Struct {
    public JniStruct() {
      super(ByteBuffer.wrap(new byte[128]));
    }
    // Basic alias type
    public final IntegerAlias m_int8 = new int8_t();
    public final IntegerAlias m_uint8 = new u_int8_t();
    public final IntegerAlias m_int16 = new int16_t();
    public final IntegerAlias m_uint16 = new u_int16_t();
    public final IntegerAlias m_int32 = new int32_t();
    public final IntegerAlias m_uint32 = new u_int32_t();
    public final IntegerAlias m_int64 = new int64_t();
    public final IntegerAlias m_uint64 = new u_int64_t();

    // Other alias type
    public final IntegerAlias m_intptr = new intptr_t();
    public final IntegerAlias m_uintptr = new uintptr_t();
    public final IntegerAlias m_caddr_t = new caddr_t();
    public final IntegerAlias m_dev = new dev_t();
    public final IntegerAlias m_blkcnt = new blkcnt_t();
    public final IntegerAlias m_blksize = new blksize_t();
    public final IntegerAlias m_gid = new gid_t();
    public final IntegerAlias m_in_addr = new in_addr_t();
    public final IntegerAlias m_in_port = new in_port_t();
    public final IntegerAlias m_ino = new ino_t();
    public final IntegerAlias m_ino64 = new ino64_t();
    public final IntegerAlias m_key = new key_t();
    public final IntegerAlias m_mode = new mode_t();
    public final IntegerAlias m_nlink = new nlink_t();
    public final IntegerAlias m_id = new id_t();
    public final IntegerAlias m_pid = new pid_t();
    public final IntegerAlias m_off = new off_t();
    public final IntegerAlias m_swblk = new swblk_t();
    public final IntegerAlias m_uid = new uid_t();
    public final IntegerAlias m_clock = new clock_t();
    public final IntegerAlias m_size = new size_t();
    public final IntegerAlias m_ssize = new ssize_t();
    public final IntegerAlias m_time = new time_t();
    public final IntegerAlias m_fsblkcnt = new fsblkcnt_t();
    public final IntegerAlias m_fsfilcnt = new fsfilcnt_t();
    public final IntegerAlias m_sa_family = new sa_family_t();
    public final IntegerAlias m_socklen = new socklen_t();
    public final IntegerAlias m_rlim = new rlim_t();
  }

  static class JnrStruct extends jnr.ffi.Struct {
    JnrStruct() {
      super(Runtime.getSystemRuntime());
    }
    public final jnr.ffi.Struct.IntegerAlias m_int8 = new jnr.ffi.Struct.int8_t();
    public final jnr.ffi.Struct.IntegerAlias m_uint8 = new jnr.ffi.Struct.u_int8_t();
    public final jnr.ffi.Struct.IntegerAlias m_int16 = new jnr.ffi.Struct.int16_t();
    public final jnr.ffi.Struct.IntegerAlias m_uint16 = new jnr.ffi.Struct.u_int16_t();
    public final jnr.ffi.Struct.IntegerAlias m_int32 = new jnr.ffi.Struct.int32_t();
    public final jnr.ffi.Struct.IntegerAlias m_uint32 = new jnr.ffi.Struct.u_int32_t();
    public final jnr.ffi.Struct.IntegerAlias m_int64 = new jnr.ffi.Struct.int64_t();
    public final jnr.ffi.Struct.IntegerAlias m_uint64 = new jnr.ffi.Struct.u_int64_t();

    public final jnr.ffi.Struct.IntegerAlias m_intptr = new jnr.ffi.Struct.intptr_t();
    public final jnr.ffi.Struct.IntegerAlias m_uintptr = new jnr.ffi.Struct.uintptr_t();
    public final jnr.ffi.Struct.IntegerAlias m_caddr_t = new jnr.ffi.Struct.caddr_t();
    public final jnr.ffi.Struct.IntegerAlias m_dev = new jnr.ffi.Struct.dev_t();
    public final jnr.ffi.Struct.IntegerAlias m_blkcnt = new jnr.ffi.Struct.blkcnt_t();
    public final jnr.ffi.Struct.IntegerAlias m_blksize = new jnr.ffi.Struct.blksize_t();
    public final jnr.ffi.Struct.IntegerAlias m_gid = new jnr.ffi.Struct.gid_t();
    public final jnr.ffi.Struct.IntegerAlias m_in_addr = new jnr.ffi.Struct.in_addr_t();
    public final jnr.ffi.Struct.IntegerAlias m_in_port = new jnr.ffi.Struct.in_port_t();
    public final jnr.ffi.Struct.IntegerAlias m_ino = new jnr.ffi.Struct.ino_t();
    public final jnr.ffi.Struct.IntegerAlias m_ino64 = new jnr.ffi.Struct.ino64_t();
    public final jnr.ffi.Struct.IntegerAlias m_key = new jnr.ffi.Struct.key_t();
    public final jnr.ffi.Struct.IntegerAlias m_mode = new jnr.ffi.Struct.mode_t();
    public final jnr.ffi.Struct.IntegerAlias m_nlink = new jnr.ffi.Struct.nlink_t();
    public final jnr.ffi.Struct.IntegerAlias m_id = new jnr.ffi.Struct.id_t();
    public final jnr.ffi.Struct.IntegerAlias m_pid = new jnr.ffi.Struct.pid_t();
    public final jnr.ffi.Struct.IntegerAlias m_off = new jnr.ffi.Struct.off_t();
    public final jnr.ffi.Struct.IntegerAlias m_swblk = new jnr.ffi.Struct.swblk_t();
    public final jnr.ffi.Struct.IntegerAlias m_uid = new jnr.ffi.Struct.uid_t();
    public final jnr.ffi.Struct.IntegerAlias m_clock = new jnr.ffi.Struct.clock_t();
    public final jnr.ffi.Struct.IntegerAlias m_size = new jnr.ffi.Struct.size_t();
    public final jnr.ffi.Struct.IntegerAlias m_ssize = new jnr.ffi.Struct.ssize_t();
    public final jnr.ffi.Struct.IntegerAlias m_time = new jnr.ffi.Struct.time_t();
    public final jnr.ffi.Struct.IntegerAlias m_fsblkcnt = new jnr.ffi.Struct.fsblkcnt_t();
    public final jnr.ffi.Struct.IntegerAlias m_fsfilcnt = new jnr.ffi.Struct.fsfilcnt_t();
    public final jnr.ffi.Struct.IntegerAlias m_sa_family = new jnr.ffi.Struct.sa_family_t();
    public final jnr.ffi.Struct.IntegerAlias m_socklen = new jnr.ffi.Struct.socklen_t();
    public final jnr.ffi.Struct.IntegerAlias m_rlim = new jnr.ffi.Struct.rlim_t();
  }

  @Test
  public void offset() {
    JniStruct jni = new JniStruct();
    JnrStruct jnr = new JnrStruct();

    assertEquals(jnr.m_int8.offset(), jni.m_int8.offset());
    assertEquals(jnr.m_uint8.offset(), jni.m_uint8.offset());
    assertEquals(jnr.m_int16.offset(), jni.m_int16.offset());
    assertEquals(jnr.m_uint16.offset(), jni.m_uint16.offset());
    assertEquals(jnr.m_int32.offset(), jni.m_int32.offset());
    assertEquals(jnr.m_uint32.offset(), jni.m_uint32.offset());
    assertEquals(jnr.m_int64.offset(), jni.m_int64.offset());
    assertEquals(jnr.m_uint64.offset(), jni.m_uint64.offset());

    assertEquals(jnr.m_intptr.offset(), jni.m_intptr.offset());
    assertEquals(jnr.m_uintptr.offset(), jni.m_uintptr.offset());
    assertEquals(jnr.m_caddr_t.offset(), jni.m_caddr_t.offset());
    assertEquals(jnr.m_dev.offset(), jni.m_dev.offset());
    assertEquals(jnr.m_blkcnt.offset(), jni.m_blkcnt.offset());
    assertEquals(jnr.m_blksize.offset(), jni.m_blksize.offset());
    assertEquals(jnr.m_gid.offset(), jni.m_gid.offset());
    assertEquals(jnr.m_in_addr.offset(), jni.m_in_addr.offset());
    assertEquals(jnr.m_in_port.offset(), jni.m_in_port.offset());
    assertEquals(jnr.m_ino.offset(), jni.m_ino.offset());
    assertEquals(jnr.m_ino64.offset(), jni.m_ino64.offset());
    assertEquals(jnr.m_key.offset(), jni.m_key.offset());
    assertEquals(jnr.m_mode.offset(), jni.m_mode.offset());
    assertEquals(jnr.m_nlink.offset(), jni.m_nlink.offset());
    assertEquals(jnr.m_id.offset(), jni.m_id.offset());
    assertEquals(jnr.m_pid.offset(), jni.m_pid.offset());
    assertEquals(jnr.m_off.offset(), jni.m_off.offset());
    assertEquals(jnr.m_swblk.offset(), jni.m_swblk.offset());
    assertEquals(jnr.m_uid.offset(), jni.m_uid.offset());
    assertEquals(jnr.m_clock.offset(), jni.m_clock.offset());
    assertEquals(jnr.m_size.offset(), jni.m_size.offset());
    assertEquals(jnr.m_ssize.offset(), jni.m_ssize.offset());
    assertEquals(jnr.m_time.offset(), jni.m_time.offset());
    assertEquals(jnr.m_fsblkcnt.offset(), jni.m_fsblkcnt.offset());
    assertEquals(jnr.m_fsfilcnt.offset(), jni.m_fsfilcnt.offset());
    assertEquals(jnr.m_sa_family.offset(), jni.m_sa_family.offset());
    assertEquals(jnr.m_socklen.offset(), jni.m_socklen.offset());
    assertEquals(jnr.m_rlim.offset(), jni.m_rlim.offset());
  }
}
