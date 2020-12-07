package alluxio.client.file.cache.submodularLib;

import alluxio.client.file.CacheParamSetter;
import alluxio.client.file.cache.Metric.ClientCacheStatistics;
import alluxio.client.file.cache.core.*;
import com.github.pasa.cache.Loader;
import com.github.pasa.cache.arc.ARCCache;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class ARCEvictor implements CachePolicy {
    public long limit = CacheParamSetter.getSpaceLimit();
    protected ClientCacheContext mContext;
    ARCCache<Pair, Object> innerCache;
    public long blockSize = CacheParamSetter.CACHE_SIZE;
    private final Object PRESENT = new Object();
    int numBlocks;
    public long hits;
    public long reqs;
    public long errs;

    @Override
    public boolean isSync() {
        return false;
    }

    @Override
    public void init(long cacheSize, ClientCacheContext context) {
        System.out.println("ARC init");
        limit = cacheSize;
        mContext = context;
        numBlocks = (int) (cacheSize / blockSize);
        System.out.println("numBlocks=" + numBlocks);
        Loader<Pair, Object> loader = new Loader<Pair, Object>() {
            @Override
            public Object load(Pair pair) {
                return PRESENT;
            }
        };
        innerCache = new ARCCache<>(numBlocks, loader);
        hits = 0;
        reqs = 0;
        errs = 0;
    }

    @Override
    public void fliter(CacheInternalUnit unit, BaseCacheUnit unit1) {
        if (unit != null) {
            unit.accessRecord.add(unit1);
        }

        this.add(unit1);
    }

    public List<Integer> getInvolvedBlock(long begin, long end) {
        long beginIndex = begin / blockSize;
        long endIndex = end / blockSize;
        if (end % blockSize == 0) {
            endIndex --;
        }

        List<Integer> res = new ArrayList<>();
        for (long i = beginIndex; i <= endIndex; i ++) {
            res.add((int)i);
        }
        return res;
    }

    public void add(CacheUnit unit) {
        List<Integer> tmp = getInvolvedBlock(unit.getBegin(), unit.getEnd());

        for (int i : tmp) {
            Pair p = new Pair(unit.getFileId(), i);
            if (innerCache.isCached(p)) {
                innerCache.get(p);
                hits ++;
                reqs ++;
            } else {
                reqs++;
                innerCache.get(p);
                addIntoCacheSpace(p.fileId, p.index);
            }
        }
    }

    public void addIntoCacheSpace(long fileId, int i) {
        CacheUnit unit = mContext.getCache(fileId, mContext.getMetedataCache().getFileLength(fileId), i * blockSize, (i +1) * blockSize, new UnlockTask());
        if (!unit.isFinish()) {
            TempCacheUnit unit1 = (TempCacheUnit) unit;
            try {
                unit1.cache(unit1.getFileId(), (int) (unit1.getEnd() - unit1.getBegin()), mContext.mFileIdToInternalList.get(fileId));
            } catch (Exception e) {
                System.out.printf("unit fd=%d, index=%d\n",
                        unit1.getFileId(), i);
                System.out.printf("hits/reqs=%f, errs=%d\n", hits/(double)reqs, errs);
                throw new RuntimeException(e);
            }
            mContext.addCache(unit1);
        }
    }

    public void removeBlockFromCachaSpace(long fileId, int i) {
        CacheUnit unit = mContext.getCache(fileId, mContext.getMetedataCache().getFileLength(fileId), i * blockSize, (i +1) * blockSize, new UnlockTask());
        if(unit.isFinish()) {
            mContext.delete((CacheInternalUnit)unit);
        }
    }

    @Override
    public void check(TempCacheUnit unit) {
        add(unit);
        if (innerCache.size() > numBlocks) {
            System.out.println("ARC cache overflow");
        }
        evict();
        ClientCacheStatistics.INSTANCE.cacheSpaceUsed = innerCache.size()  * blockSize;
    }

    @Override
    public long evict() {
        LinkedList<Pair> victims = innerCache.victims;
        for (Pair v :  victims) {
//            if (v.equals(p)) {
//                System.out.printf("ERROR: victim(%d, %d) is p(%d,%d)\n",
//                        v.fileId, v.index, p.fileId, p.index);
//                System.out.printf("innerCache, size/capacity=%d/%d\n", innerCache.size(), innerCache.getCapacity());
//            }
            removeBlockFromCachaSpace(v.fileId, v.index);
        }
        if (victims.size() > 0) {
//            System.out.println("ARC evict " + victims.size());
        }
        victims.clear();
        return 0;
    }

    @Override
    public void clear() {

    }

    @Override
    public PolicyName getPolicyName() {
        return PolicyName.ARC;
    }

    @Override
    public boolean isFixedLength() {
        return true;
    }

    static class Pair {
        long fileId;
        int index;

        public Pair(long id, int index) {
            fileId = id;
            this.index = index;
        }

//        @Override
//        public int hashCode() {
//            return (int)(fileId * 31 + index) * 31;
//        }


        @Override
        public int hashCode() {
            return Objects.hash(fileId, index);
        }

        @Override
        public boolean equals(Object obj) {
            if (! (obj instanceof Pair)) {
                return false;
            }
            Pair p = (Pair)obj;
            return fileId == p.fileId && index == p.index;
        }
    }

}
