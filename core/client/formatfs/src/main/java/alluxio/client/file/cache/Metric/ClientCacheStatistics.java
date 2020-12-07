package alluxio.client.file.cache.Metric;

public enum ClientCacheStatistics {
    INSTANCE;

    public long bytesHit;
    public long bytesRead;

    public long readUFSTime;
    public long readCacheTime;
    public long copyBufferTime;
    public long getCacheTime;
    public long lockTime;
    public long readUnitTime;
    public long readTmpUnitTime;
    public long lazyReadTime;
    public long evictTime;
    public long testTime;

    public long cacheSpaceUsed;

    public void clear() {
        bytesHit = 0;
        bytesRead = 0;
        readUFSTime = 0;
        readCacheTime = 0;
        copyBufferTime = 0;
        getCacheTime = 0;
        lockTime = 0;
        readUnitTime = 0;
        readTmpUnitTime = 0;
        lazyReadTime = 0;
        evictTime = 0;
        testTime = 0;
        cacheSpaceUsed = 0;
    }

    public double hitRatio() {
        return bytesHit / (double) bytesRead;
    }

    @Override
    public String toString() {
        return "ClientCacheStatistics{" +
                "bytesHit=" + bytesHit +
                ", bytesRead=" + bytesRead +
                ", readUFSTime=" + readUFSTime +
                ", readCacheTime=" + readCacheTime +
                ", copyBufferTime=" + copyBufferTime +
                ", getCacheTime=" + getCacheTime +
                ", lockTime=" + lockTime +
                ", readUnitTime=" + readUnitTime +
                ", readTmpUnitTime=" + readTmpUnitTime +
                ", lazyReadTime=" + lazyReadTime +
                ", evictTime=" + evictTime +
                ", testTime=" + testTime +
                ", cacheSpaceUsed=" + cacheSpaceUsed +
                '}';
    }
}
