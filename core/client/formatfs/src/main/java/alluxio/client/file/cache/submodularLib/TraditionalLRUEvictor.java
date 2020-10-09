package alluxio.client.file.cache.submodularLib;

import alluxio.client.file.CacheParamSetter;
import alluxio.client.file.cache.core.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

//public class TraditionalLRUEvictor extends TraditionalLFUEvictor {
public class TraditionalLRUEvictor implements CachePolicy {
    private Set<pair> s = new HashSet<>();
    private LinkedList<pair> visitMap = new LinkedList<>();
//    public long limit = 1024 * 1024 * 1000;
    public long limit = CacheParamSetter.getSpaceLimit();
    public long blockSize = CacheParamSetter.CACHE_SIZE;
    protected ClientCacheContext mContext;
    protected Set<BaseCacheUnit> visitList = new HashSet<>();
    public long mTestFileId = 1;

    public TraditionalLRUEvictor() {
    }


    public PolicyName getPolicyName() {
        return PolicyName.TRADITIONAL_LRU;
    }

    @Override
    public boolean isFixedLength() {
        return true;
    }

    @Override
    public void check(TempCacheUnit unit) {
        add(unit);
//        long newSize = getNewSize(unit);
        if (visitMap.size() * blockSize > limit) {
            evict();
        }
    }
    public long evict() {
        long size = visitMap.size() * blockSize;
        long deleteSize = 0;
        while (size > limit) {
            //System.out.println(size / (1024  *1024));
            pair p = visitMap.pollLast();
            s.remove(p);
            removeBlockFromCachaSpace(p.fileId, p.index);

            deleteSize += blockSize;
            size = visitMap.size() * blockSize;
        }
        return deleteSize;
    }

    public void removeBlockFromCachaSpace(long fileId, int i) {
        CacheUnit unit = mContext.getCache(fileId, mContext.getMetedataCache().getFileLength(fileId), i * blockSize, (i +1) * blockSize, new UnlockTask());
        if(unit.isFinish()) {
            mContext.delete((CacheInternalUnit)unit);
        }
    }

    @Override
    public void clear() {

    }


    @Override
    public boolean isSync() {
        return false;
    }

    @Override
    public void init(long cacheSize, ClientCacheContext context) {
        limit = cacheSize;
        mContext = context;
    }

    @Override
    public void fliter(CacheInternalUnit unit, BaseCacheUnit unit1) {
        if (unit != null) {
            unit.accessRecord.add(unit1);
        }

        this.add(unit1);
    }
    public void fakeFliter(BaseCacheUnit unit1) {

        this.fakeAdd(unit1);
    }
    public void fakeCheck() {
//        long newSize = getNewSize(unit);
        if (visitMap.size() * blockSize > limit) {
            fakeEvict();
        }
    }
    public long fakeEvict() {
        long size = visitMap.size() * blockSize;
        long deleteSize = 0;
        while (size > limit) {
            //System.out.println(size / (1024  *1024));
            pair p = visitMap.pollFirst();
            s.remove(p);

            deleteSize += blockSize;
            size = visitMap.size() * blockSize;
        }
        return deleteSize;
    }

    public void fakeAdd(CacheUnit unit) {
        List<Integer> tmp = getInvolvedBlock(unit.getBegin(), unit.getEnd());

        for (int i : tmp) {
            pair p = new pair(unit.getFileId(), i);
            if (s.contains(p)) {
                visitMap.remove(p);
                visitMap.addLast(p);
            } else {
                visitMap.addLast(p);
                s.add(p);
            }
        }
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
            pair p = new pair(unit.getFileId(), i);
            if (s.contains(p)) {
                visitMap.remove(p);
                visitMap.addFirst(p);
            } else {
                visitMap.addFirst(p);
                s.add(p);
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
                throw new RuntimeException(e);
            }
            mContext.addCache(unit1);
        }
    }

    public void init(long limit) throws Exception {
        String fileName = "/home/alex/cache-exp/msr-cambridge1/MSR-Cambridge/proj_0.csv";
        File file = new File(fileName);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String tempString;
        while ((tempString = reader.readLine()) != null) {
            String[] content = tempString.split(",");
            if (content[3].equals("Write")) {
                continue;
            }

            long offset = Long.parseLong(content[4]);
            int size = Integer.parseInt(content[5]);

            BaseCacheUnit unit = new BaseCacheUnit(mTestFileId, offset, offset + size);
            visitList.add(unit);
//            add(unit);
        }
        reader.close();
//        long sum = 0;
//        for(int i = 0 ; i < 1200; i ++) {
//            long length = RandomUtils.nextLong(1024 * 1024, 1024 * 1024 * 4);
//            long begin = RandomUtils.nextLong(0, 1024 * 1024 * 1024 - length);
//            sum += length;
//            BaseCacheUnit unit = new BaseCacheUnit(mTestFileId, begin, begin + length);
//            visitList.add(unit);
//            add(unit);
//        }
//        evict();
//        System.out.println(visitMap.size() * blockSize / (1024 *1024));
    }
    public void testClean() {
        visitMap.clear();
        s.clear();
    }
    public void testVisit() {
        double visitTime = 0;
        long visitSize = 0;
        long allVisitSize = 0;
        int count = 0;
        for (BaseCacheUnit unit : visitList) {
            if (count % 10000 == 0) {
                System.out.println(count);
            }
            List<Integer> tmp = getInvolvedBlock(unit.getBegin(), unit.getEnd());
            long fileId = unit.getFileId();
            boolean rightLarge, leftSmall;
            if (tmp.size() == 0) {
                rightLarge = leftSmall= false;
            } else {
                rightLarge = unit.getEnd() > (tmp.get(tmp.size() - 1) + 1) * blockSize;
                leftSmall = unit.getBegin() < tmp.get(0) * blockSize + 1;
            }
            for (int i : tmp) {
                if (visitMap.contains(new pair(fileId, i))) {
                    visitSize += blockSize;
                }
            }
            if (leftSmall && visitMap.contains(new pair(fileId, tmp.get(0) - 1))) {
                visitSize += (tmp.get(0) ) * blockSize - unit.getBegin();
                // System.out.println("left : " + (double)((tmp.get(0) ) * blockSize - unit.getBegin()) / (double) (1024 * 1024));
            }
            if (rightLarge && visitMap.contains(new pair(fileId, tmp.get(tmp.size() - 1) + 1))) {
                visitSize += unit.getEnd() -( tmp.get(tmp.size() - 1)  +1 )* blockSize;
                //System.out.println("right " + (double)(unit.getEnd() -( tmp.get(tmp.size() - 1)  +1 )* blockSize) / (double) (1024 * 1024));

            }
            allVisitSize += unit.getSize();
            fakeFliter(unit);
            fakeCheck();
            count ++;
        }
        System.out.println("hitRatio by size : " + ((double) visitSize / (double) allVisitSize));

    }
    public static void main(String[] args) throws Exception{
        TraditionalLRUEvictor test = new TraditionalLRUEvictor();
        test.limit = 1024 * 1024 * 2000;
        test.blockSize = 102400;
//        test.blockSize = 1048576;
        test.init(1024 * 1024 * 2000);
        test.testVisit();
    }

    class pair {
        long fileId;
        int index;

        public pair(long id, int index) {
            fileId = id;
            this.index = index;
        }

        @Override
        public int hashCode() {
            return (int)(fileId * 31 + index) * 31;
        }

        @Override
        public boolean equals(Object obj) {
            if (! (obj instanceof pair)) {
                return false;
            }
            pair p = (pair)obj;
            return fileId == p.fileId && index == p.index;
        }
    }



}
