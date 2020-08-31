package alluxio.jnifuse.struct;

import alluxio.util.OSUtils;

public enum NativeType {
    VOID(0, 0),
    SCHAR(1, 1),
    UCHAR(1, 1),
    SSHORT(2, 2),
    USHORT(2, 2),
    SINT(4, 4),
    UINT(4, 4),
    /** TODO: long type should depend on os */
    SLONG(8, 8),
    ULONG(8, 8),
    SLONGLONG(8, 8),
    ULONGLONG(8, 8),
    FLOAT(4, 4),
    DOUBLE(8, 8),
    STRUCT(0, 0),
    ADDRESS(ULONG);

    /** The number of bits of this native type should align. */
    private final int mAlignment;

    /** The number of bits this native type contains. */
    private final int mSize;

    NativeType(int size, int alignment) {
        mSize = size;
        mAlignment = alignment;
    }

    NativeType(NativeType type) {
        mSize = type.mSize;
        mAlignment = type.mAlignment;
    }

    public int getAlignment() {
        /* Workround for native long. */
        if (this.equals(SLONG) || this.equals(ULONG)) {
            if (OSUtils.is64Bit()) {
                return 8;
            } else {
                return 4;
            }
        }
        return mAlignment;
    }

    public int getSize() {
        /* Workround for native long. */
        if (this.equals(SLONG) || this.equals(ULONG)) {
            if (OSUtils.is64Bit()) {
                return 8;
            } else {
                return 4;
            }
        }
        return mSize;
    }
}
