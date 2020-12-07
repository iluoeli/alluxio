package alluxio.client.file.cache.submodularLib;

import alluxio.client.file.CacheParamSetter;
import alluxio.client.file.cache.Metric.ClientCacheStatistics;
import alluxio.client.file.cache.core.*;
import alluxio.collections.Pair;
import org.apache.commons.lang3.RandomUtils;

import java.util.*;

public class TraditionalLFUEvictor implements CachePolicy {
    public PriorityQueue<TempCacheUnit> mVisitQueue;
    private Map<Long, Map<Integer, KeyWithPriority>> visitMap = new HashMap<>();
    private PriorityQueue<KeyWithPriority> pq = new PriorityQueue<>();
    private long now = 0;
//    public long blockSize = (long) (1024 * 1024 * 2 );
    public long blockSize = CacheParamSetter.CACHE_SIZE;
    private double mVisitSize = 0;
    private double mHitSize = 0;
//    public long limit = 1024 * 1024 * 400;
    public long limit = CacheParamSetter.getSpaceLimit();
    public long mPromoteSize = 0;
    protected Set<BaseCacheUnit> visitList = new HashSet<>();
    public long mTestFileId = 1;
    protected ClientCacheContext mContext;


    public boolean isSync() {
        return false;
    }

    public void init(long cacheSize, ClientCacheContext context) {
        limit = cacheSize;
        mContext =context;
//        mContext.stopCache();
    }

    public void fliter(CacheInternalUnit unit, BaseCacheUnit unit1) {
        if (unit != null) {
            unit.accessRecord.add(unit1);
        }
        add(unit1);
    }

    public void check(TempCacheUnit unit) {
        add(unit);
//        long newSize = getNewSize(unit);
        if (getBlockNum() * blockSize > limit) {
            evict();
        }
        ClientCacheStatistics.INSTANCE.cacheSpaceUsed = getBlockNum() * blockSize;
    }

    public void clear() {

    }

    public PolicyName getPolicyName() {
        return PolicyName.TRADITIONAL_LFU;
    }

    @Override
    public boolean isFixedLength() {
        return true;
    }


    public TraditionalLFUEvictor() {
        mVisitQueue = new PriorityQueue<>();
    }
    //0-10 11-20 21-30 31-40 25 35
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

    public int getBlockNum() {
        if (visitMap.isEmpty()) return 0;
        int res = 0;
        for( long l : visitMap.keySet()) {
            res += visitMap.get(l).size();
        }
        return res;
    }

    public long evict() {
        long blockNum = getBlockNum();
        long size = blockNum * blockSize;
        long delete = 0;
        while (size > limit) {
            // System.out.println(size / (1024  *1024));
//            int minT = -1;
//            long minFileId = -1;
//            int min = Integer.MAX_VALUE;
//            for (long l : visitMap.keySet()) {
//                for (int i : visitMap.get(l).keySet()) {
//                    if (visitMap.get(l).get(i) < min) {
//                        minT = i;
//                        min = visitMap.get(l).get(i);
//                        minFileId = l;
//                    }
//                }
//            }
            KeyWithPriority kp = pq.poll();
            if (kp == null) {
                System.out.println("pq should not be null");
            } else {
                long minFileId = kp.fd;
                int minT = kp.id;
                if (visitMap.containsKey(minFileId)) {
                    visitMap.get(minFileId).remove(minT);
                    delete += blockSize;
                    blockNum --;
                    size = blockNum * blockSize;
                    removeBlockFromCachaSpace(minFileId, minT);
                }
            }
        }
        return delete;
    }

    public void removeBlockFromCachaSpace(long fileId, int i) {
        CacheUnit unit = mContext.getCache(fileId, mContext.getMetedataCache().getFileLength(fileId), i * blockSize, (i +1) * blockSize, new UnlockTask());
        if(unit.isFinish()) {
            mContext.delete((CacheInternalUnit)unit);
        }
    }

    private void addUnit(long fileId, int i) {
        if (!visitMap.containsKey(fileId)) {
            visitMap.put(fileId, new HashMap<>());
        }
        if (visitMap.get(fileId).containsKey(i)) {
            KeyWithPriority kp = visitMap.get(fileId).get(i);
            pq.remove(kp);
            kp.hits++;
            kp.timestamp = now++;
            pq.offer(kp);
//            visitMap.get(fileId).put(i, kp);
        } else {
            KeyWithPriority kp = new KeyWithPriority(i, fileId,0, now++);
            visitMap.get(fileId).put(i, kp);
            pq.offer(kp);

            mPromoteSize += blockSize;
        }
    }

    public void add(CacheUnit unit) {
        List<Integer> tmp = getInvolvedBlock(unit.getBegin(), unit.getEnd());
        for (int i : tmp) {
            //System.out.println(i);
            addUnit(unit.getFileId(), i);
            addIntoCacheSpace(unit.getFileId(), i);

        }
    }

    public void addIntoCacheSpace(long fileId, int i) {
        CacheUnit unit = mContext.getCache(fileId, mContext.getMetedataCache().getFileLength(fileId), i * blockSize, (i +1) * blockSize, new UnlockTask());
        if (!unit.isFinish()) {
            TempCacheUnit unit1 = (TempCacheUnit) unit;
            try {
                unit1.cache(unit1.getFileId(), (int) (unit1.getEnd() - unit1.getBegin()), mContext.mFileIdToInternalList.get(fileId));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            mContext.addCache(unit1);
        }
    }

    public void init(long limit) throws Exception {
        long sum = 0;
        for(int i = 0 ; i < 1200; i ++) {
            long length = RandomUtils.nextLong(1024 * 8, 1024 * 1024 * 4);
            long begin = RandomUtils.nextLong(0, 1024 * 1024 * 1024 - length);
            sum += length;
            BaseCacheUnit unit = new BaseCacheUnit(mTestFileId, begin, begin + length);
            visitList.add(unit);
            //add(unit);
        }
        System.out.println(visitMap.size() * blockSize / (1024 *1024));
    }

    long getNewSize(CacheUnit unit) {
        boolean rightLarge, leftSmall;
        List<Integer> tmp = getInvolvedBlock(unit.getBegin(), unit.getEnd());
        if (tmp.size() == 0) {
            rightLarge = leftSmall= false;
        } else {
            rightLarge = unit.getEnd() > (tmp.get(tmp.size() - 1) + 1) * blockSize;
            leftSmall = unit.getBegin() < tmp.get(0) * blockSize + 1;
        }
        long hitSize = 0;
        for (int i : tmp) {
            if (visitMap.containsKey(unit.getFileId()) && visitMap.get(unit.getFileId()).containsKey(i)) {
                hitSize += blockSize;
            }
        }
        if (leftSmall && visitMap.containsKey(unit.getFileId()) && visitMap.get(unit.getFileId()).containsKey(tmp.get(0) - 1)) {
            hitSize += (tmp.get(0) ) * blockSize - unit.getBegin();
            // System.out.println("left : " + (double)((tmp.get(0) ) * blockSize - unit.getBegin()) / (double) (1024 * 1024));
        }
        if (rightLarge && visitMap.containsKey(unit.getFileId()) && visitMap.get(unit.getFileId()).containsKey(tmp.get(tmp.size() - 1) + 1)) {
            hitSize += unit.getEnd() -( tmp.get(tmp.size() - 1)  +1 )* blockSize;
            //System.out.println("right " + (double)(unit.getEnd() -( tmp.get(tmp.size() - 1)  +1 )* blockSize) / (double) (1024 * 1024));
        }
        return unit.getSize() - hitSize;
    }

    public void testVisit() {
        double visitTime = 0;
        for (BaseCacheUnit unit : visitList) {
            mVisitSize += unit.getSize();
            List<Integer> tmp = getInvolvedBlock(unit.getBegin(), unit.getEnd());

            boolean rightLarge, leftSmall;
            if (tmp.size() == 0) {
                rightLarge = leftSmall= false;
            } else {
                rightLarge = unit.getEnd() > (tmp.get(tmp.size() - 1) + 1) * blockSize;
                leftSmall = unit.getBegin() < tmp.get(0) * blockSize + 1;
            }
            for (int i : tmp) {
                if (visitMap.containsKey(unit.getFileId()) && visitMap.get(unit.getFileId()).containsKey(i)) {
                    mHitSize += blockSize;
                }
            }
            if (leftSmall && visitMap.containsKey(unit.getFileId()) && visitMap.get(unit.getFileId()).containsKey(tmp.get(0) - 1)) {
                mHitSize += (tmp.get(0) ) * blockSize - unit.getBegin();
                // System.out.println("left : " + (double)((tmp.get(0) ) * blockSize - unit.getBegin()) / (double) (1024 * 1024));
            } else {
                if (tmp.size() > 0 && tmp.get(0) - 1 >= 0)
                    addUnit(unit.getFileId(), tmp.get(0) - 1);
            }
            if (rightLarge && visitMap.containsKey(unit.getFileId()) && visitMap.get(unit.getFileId()).containsKey(tmp.get(tmp.size() - 1) + 1)) {
                mHitSize += unit.getEnd() -( tmp.get(tmp.size() - 1)  +1 )* blockSize;
                //System.out.println("right " + (double)(unit.getEnd() -( tmp.get(tmp.size() - 1)  +1 )* blockSize) / (double) (1024 * 1024));
            } else {
                if (tmp.size() > 0 ) {
                    addUnit(unit.getFileId(), tmp.get(tmp.size() - 1) + 1);
                }
            }
            add(unit);
            evict();
        }
        System.out.println("hitRatio by size : " + ((double) mHitSize / (double) mVisitSize));
        System.out.println("additional overhead " + mPromoteSize / mVisitSize);
        System.out.println(mPromoteSize/ (1024 * 1024));
        mPromoteSize = 0;
    }

    public static void main(String[] args) throws Exception{
        TraditionalLFUEvictor test = new TraditionalLFUEvictor();
        test.init(1024 * 1024 * 400);
        test.testVisit();
        test.mVisitSize = test.mHitSize = 0;
        test.testVisit();
    }

    static class KeyWithPriority implements Comparable<KeyWithPriority> {
        long hits;
        long timestamp;
        int id;
        long fd;

        public KeyWithPriority(int id, long fd, long hits, long timestamp) {
            this.id = id;
            this.fd = fd;
            this.hits = hits;
            this.timestamp = timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            KeyWithPriority that = (KeyWithPriority) o;
            return hits == that.hits &&
                    timestamp == that.timestamp &&
                    id == that.id &&
                    fd == that.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(hits, timestamp, id, fd);
        }

        @Override
        public int compareTo(KeyWithPriority other) {
            if (this.hits == other.hits) {
                return (int) (this.timestamp - other.timestamp);
            }
            return (int) (this.hits - other.hits);
        }
    }
}
