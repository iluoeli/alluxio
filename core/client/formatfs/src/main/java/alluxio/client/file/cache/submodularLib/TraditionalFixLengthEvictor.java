package alluxio.client.file.cache.submodularLib;

import alluxio.client.file.cache.core.*;

public class TraditionalFixLengthEvictor implements CachePolicy {

    @Override
    public boolean isSync() {
        return false;
    }

    @Override
    public void init(long cacheSize, ClientCacheContext context) {

    }

    @Override
    public void fliter(CacheInternalUnit unit, BaseCacheUnit unit1) {

    }

    @Override
    public void check(TempCacheUnit unit) {

    }

    @Override
    public long evict() {
        return 0;
    }

    @Override
    public void clear() {

    }

    @Override
    public PolicyName getPolicyName() {
        return null;
    }

    @Override
    public boolean isFixedLength() {
        return false;
    }
}
