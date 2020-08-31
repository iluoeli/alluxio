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

import alluxio.jnifuse.utils.Platform;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public abstract class Struct {
  static final Charset ASCII = StandardCharsets.US_ASCII;
  static final Charset UTF8 = StandardCharsets.UTF_8;

  public ByteBuffer buffer;

  static final class Info {
    int size = 0;
    int minAlign = 1;

    private static int align(int offset, int align) {
      return (offset + align - 1) & ~(align - 1);
    }

    protected final int addField(int sizeBits, int alignBits) {
      final int alignment = alignBits >> 3;
      final int offset = align(this.size, alignment);
      this.size = Math.max(this.size, offset + (sizeBits >> 3));
      this.minAlign = Math.max(this.minAlign, alignment);
      return offset;
    }
  }

  final Info _info;

  protected Struct(ByteBuffer bb) {
    this._info = new Info();
    this.buffer = bb;
    bb.order(ByteOrder.LITTLE_ENDIAN);
  }

  protected abstract static class Member {
    abstract int offset();
  }

  public abstract class NumberField extends Member {
    private final int offset;
    protected NativeType nativeType;

    protected NumberField() {
      this.offset = _info.addField(getSize() * 8, getAlignment() * 8);
    }

    protected NumberField(TypeAlias type) {
      this.nativeType = Platform.findNativeType(type);
      this.offset = _info.addField(getSize() * 8, getAlignment() * 8);
    }

    public final int offset() {
      return offset;
    }

    protected abstract int getSize();

    protected abstract int getAlignment();
  }

  public class Signed16 extends NumberField {
    @Override
    protected int getSize() {
      return 2;
    }

    @Override
    protected int getAlignment() {
      return 2;
    }

    public final short get() {
      return buffer.getShort(offset());
    }

    public final void set(short value) {
      buffer.putShort(offset(), value);
    }
  }

  public class Unsigned16 extends NumberField {

    @Override
    protected int getSize() {
      return 2;
    }

    @Override
    protected int getAlignment() {
      return 2;
    }

    public final int get() {
      int value = buffer.getShort(offset());
      return value < 0 ? (int) ((value & 0x7FFF) + 0x8000) : value;
    }

    public final void set(int value) {
      buffer.putShort(offset(), (short) value);
    }
  }

  public class Signed32 extends NumberField {

    @Override
    protected int getSize() {
      return 4;
    }

    @Override
    protected int getAlignment() {
      return 4;
    }

    public final void set(int value) {
      buffer.putInt(offset(), value);
    }

    public final int get() {
      return buffer.getInt(offset());
    }
  }

  public class Unsigned32 extends NumberField {
    @Override
    protected int getSize() {
      return 4;
    }

    @Override
    protected int getAlignment() {
      return 4;
    }

    public final long get() {
      long value = buffer.getInt(offset());
      return value < 0 ? (long) ((value & 0x7FFFFFFFL) + 0x80000000L) : value;
    }

    public final void set(long value) {
      buffer.putInt(offset(), (int) value);
    }

    public final int intValue() {
      return (int) get();
    }
  }

  public class Signed64 extends NumberField {

    @Override
    protected int getSize() {
      return 8;
    }

    @Override
    protected int getAlignment() {
      return 8;
    }

    public final long get() {
      return buffer.getLong(offset());
    }

    public final void set(long value) {
      buffer.putLong(offset(), value);
    }
  }

  public class Unsigned64 extends NumberField {

    @Override
    protected int getSize() {
      return 8;
    }

    @Override
    protected int getAlignment() {
      return 8;
    }

    public final long get() {
      return buffer.getLong(offset());
    }

    public final void set(long value) {
      buffer.putLong(offset(), value);
    }
  }

  public class SignedLong extends NumberField {

    @Override
    protected int getSize() {
      return 8;
    }

    @Override
    protected int getAlignment() {
      return 8;
    }

    public final long get() {
      return buffer.getLong(offset());
    }

    public final void set(long value) {
      buffer.putLong(offset(), value);
    }

    public final int intValue() {
      return (int) get();
    }

    public final long longValue() {
      return get();
    }
  }

  public class UnsignedLong extends NumberField {

    @Override
    protected int getSize() {
      return 8;
    }

    @Override
    protected int getAlignment() {
      return 8;
    }

    public final long get() {
      return buffer.getLong(offset());
    }

    public final void set(long value) {
      buffer.putLong(offset(), value);
    }

    public final int intValue() {
      return (int) get();
    }

    public final long longValue() {
      return get();
    }
  }

  protected abstract class AbstrctMember extends Member {
    private final int offset;

    protected AbstrctMember(int size) {
      this.offset = _info.addField(size * 8, 8);
    }

    @Override
    public final int offset() {
      return offset;
    }
  }

  public final class Padding extends AbstrctMember {
    public Padding(int size) {
      super(size);
    }
  }

  public final class Timespec extends Member {
    public final SignedLong tv_sec;
    public final SignedLong tv_nsec;
    public final int offset;

    protected Timespec() {
      // TODO: this may cause error
      tv_sec = new SignedLong();
      tv_nsec = new SignedLong();
      offset = tv_sec.offset();
    }

    @Override
    int offset() {
      return offset;
    }

    protected int getSize() {
      return tv_sec.getSize() + tv_nsec.getSize();
    }

    protected int getAlignment() {
      return Math.max(tv_sec.getAlignment(), tv_nsec.getAlignment());
    }
  }

  /**
   * Integer alias type which is mapped to a specific native type.
   * They are platform-dependent, so we have to get their info during runtime.
   * Including: {
   * int8_t, u_int8_t, int16_t, u_int16_t,
   * int32_t, u_int32_t,  int64_t, u_int64_t,
   * intptr_t, uintptr_t, caddr_t, dev_t,
   * blkcnt_t, blksize_t, gid_t, in_addr_t,
   * in_port_t, ino_t, ino64_t, key_t,
   * mode_t, nlink_t, id_t, pid_t,
   * off_t, swblk_t, uid_t, clock_t,
   * size_t, ssize_t, time_t, fsblkcnt_t,
   * fsfilcnt_t, sa_family_t, socklen_t, rlim_t
   * }
   */
  public class IntegerAlias extends NumberField {
    IntegerAlias(TypeAlias type) {
      super(type);
    }

    @Override
    protected int getSize() {
      return nativeType.getSize();
    }

    @Override
    public int getAlignment() {
      return nativeType.getAlignment();
    }

    public void setByte(byte value) {
      buffer.put(offset(), value);
    }

    public byte byteValue() {
      return buffer.get(offset());
    }

    public void setShort(short value) {
      buffer.putShort(offset(), value);
    }

    public short shortValue() {
      return buffer.getShort(offset());
    }

    public void setInt(int value) {
      buffer.putInt(offset(), value);
    }

    public int intValue() {
      return buffer.getInt(offset());
    }

    public void setLong(long value) {
      buffer.putLong(offset(), value);
    }

    public long longValue() {
      return buffer.getLong(offset());
    }

    public void set(long value) {
      int bits = getSize();
      switch (bits) {
        case 1:
          setByte((byte) value); break;
        case 2: setShort((short) value); break;
        case 4: setInt((int) value); break;
        case 8:
        default: setLong(value);
      }
    }

    public long get() {
      int bits = getSize() * 8;
      long value = longValue();
      return value & (0xffffffffffffffffL >>> (64-bits));
    }
  }

  public final class int8_t extends IntegerAlias {
    int8_t() { super(TypeAlias.int8_t); }
  }

  public final class u_int8_t extends IntegerAlias {
    u_int8_t() { super(TypeAlias.u_int8_t); }
  }

  public final class int16_t extends IntegerAlias {
    int16_t() { super(TypeAlias.int16_t); }
  }

  public final class u_int16_t extends IntegerAlias {
    u_int16_t() { super(TypeAlias.u_int16_t); }
  }

  public final class int32_t extends IntegerAlias {
    int32_t() { super(TypeAlias.int32_t); }
  }

  public final class u_int32_t extends IntegerAlias {
    u_int32_t() { super(TypeAlias.u_int32_t); }
  }

  public final class int64_t extends IntegerAlias {
    int64_t() { super(TypeAlias.int64_t); }
  }

  public final class u_int64_t extends IntegerAlias {
    u_int64_t() { super(TypeAlias.u_int64_t); }
  }

  public final class intptr_t extends IntegerAlias {
    intptr_t() { super(TypeAlias.intptr_t); }
  }

  public final class uintptr_t extends IntegerAlias {
    public uintptr_t() { super(TypeAlias.uintptr_t); }
  }

  public final class caddr_t extends IntegerAlias {
    public caddr_t() { super(TypeAlias.caddr_t); }
  }

  public final class dev_t extends IntegerAlias {
    public dev_t() { super(TypeAlias.dev_t); }
  }

  public final class blkcnt_t extends IntegerAlias {
    public blkcnt_t() { super(TypeAlias.blkcnt_t); }
  }

  public final class blksize_t extends IntegerAlias {
    public blksize_t() { super(TypeAlias.blksize_t); }
  }

  public final class gid_t extends IntegerAlias {
    public gid_t() { super(TypeAlias.gid_t); }
  }

  public final class in_addr_t extends IntegerAlias {
    public in_addr_t() { super(TypeAlias.in_addr_t); }
  }

  public final class in_port_t extends IntegerAlias {
    public in_port_t() { super(TypeAlias.in_port_t); }
  }

  public final class ino_t extends IntegerAlias {
    public ino_t() { super(TypeAlias.ino_t); }
  }

  public final class ino64_t extends IntegerAlias {
    public ino64_t() { super(TypeAlias.ino64_t); }
  }

  public final class key_t extends IntegerAlias {
    public key_t() { super(TypeAlias.key_t); }
  }

  public final class mode_t extends IntegerAlias {
    public mode_t() { super(TypeAlias.mode_t); }
  }

  public final class nlink_t extends IntegerAlias {
    public nlink_t() { super(TypeAlias.nlink_t); }
  }

  public final class id_t extends IntegerAlias {
    public id_t() { super(TypeAlias.id_t); }
  }

  public final class pid_t extends IntegerAlias {
    public pid_t() { super(TypeAlias.pid_t); }
  }

  public final class off_t extends IntegerAlias {
    public off_t() { super(TypeAlias.off_t); }
  }

  public final class swblk_t extends IntegerAlias {
    public swblk_t() { super(TypeAlias.swblk_t); }
  }

  public final class uid_t extends IntegerAlias {
    public uid_t() { super(TypeAlias.uid_t); }
  }

  public final class clock_t extends IntegerAlias {
    public clock_t() { super(TypeAlias.clock_t); }
  }

  public final class size_t extends IntegerAlias {
    public size_t() { super(TypeAlias.size_t); }
  }

  public final class ssize_t extends IntegerAlias {
    public ssize_t() { super(TypeAlias.ssize_t); }
  }

  public final class time_t extends IntegerAlias {
    public time_t() { super(TypeAlias.time_t); }
  }

  public final class fsblkcnt_t extends IntegerAlias {
    public fsblkcnt_t() { super(TypeAlias.fsblkcnt_t); }
  }

  public final class fsfilcnt_t extends IntegerAlias {
    public fsfilcnt_t() { super(TypeAlias.fsfilcnt_t); }
  }

  public final class sa_family_t extends IntegerAlias {
    public sa_family_t() { super(TypeAlias.sa_family_t); }
  }

  public final class socklen_t extends IntegerAlias {
    public socklen_t() { super(TypeAlias.socklen_t); }
  }

  public final class rlim_t extends IntegerAlias {
    public rlim_t() { super(TypeAlias.rlim_t); }
  }

}
