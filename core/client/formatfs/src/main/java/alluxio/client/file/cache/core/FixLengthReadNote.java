package alluxio.client.file.cache.core;

// This class is used for proper calculation of hit size when reading with fixed length policy
// (TraditionalLRU and TraditionalLFU)
// As the fixed length policy would require reading whole blocks, the real required pos and length can be recorded here
public class FixLengthReadNote {
    public static boolean isIsUsingNote() {
        return isUsingNote;
    }

    private static boolean isUsingNote = false;
    public static long realPos;
    public static int realLen;

    public static void takeNote(long pos, int len) {
        realPos = pos;
        realLen = len;
        isUsingNote = true;
    }

    public static void discardNote() {
        isUsingNote = false;
    }

    public static int realHitLen(long pos, int len) {
        long begin = pos, end = pos + len;
        long realBegin = realPos, realEnd = realPos + realLen;
        long b = Math.min(end, realEnd), a = Math.max(begin, realBegin);
        return ((b-a) > 0) ? (int)(b-a) : 0;
    }
}
