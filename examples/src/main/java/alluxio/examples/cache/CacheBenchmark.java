package alluxio.examples.cache;

import alluxio.AlluxioURI;
import alluxio.client.file.*;
import alluxio.client.file.cache.Metric.ClientCacheStatistics;
import alluxio.client.file.cache.core.*;
import org.apache.commons.lang3.RandomUtils;

public class CacheBenchmark {

    //  0               1               2               3               4       5
    // <alluxioURI>  <cachePolicy> <cacheCapacity> <blockSize>        <mode>   <giga>
    public static void main(String[] args) {
        if (args.length != 7) {
            System.out.println("Usage: <alluxioURI>  <cachePolicy> <cacheCapacity> <blockSize>        <mode>   <giga>");
            System.exit(1);
        }
        AlluxioURI alluxioURI = new AlluxioURI(args[0]);
        String policy = args[1];
        long cachePacity = Long.parseLong(args[2]);
        int blockSize = Integer.parseInt(args[3]);
        String mode = args[4];
        int cacheInGiga = Integer.parseInt(args[5]);

        initSetting(policy, cachePacity, blockSize, mode);

        randomTest(alluxioURI, cacheInGiga);

        System.gc();
        System.exit(0);
    }

    static void initSetting(String policy, long cacheCapacity, int blockSize, String mode) {
        if (mode.equals("promote")) {
            CacheParamSetter.mode = ClientCacheContext.MODE.PROMOTE;
        } else {
            CacheParamSetter.mode = ClientCacheContext.MODE.EVICT;
        }
        CacheParamSetter.CACHE_SIZE = blockSize;
        switch (policy) {
            case "isk":
                CacheParamSetter.POLICY_NAME = CachePolicy.PolicyName.ISK;
                break;
            case "gr":
                CacheParamSetter.POLICY_NAME = CachePolicy.PolicyName.GR;
                break;
            case "lfu":
                CacheParamSetter.POLICY_NAME = CachePolicy.PolicyName.TRADITIONAL_LFU;
                break;
            case "lru":
                CacheParamSetter.POLICY_NAME = CachePolicy.PolicyName.TRADITIONAL_LRU;
                break;
            case "divide":
                CacheParamSetter.POLICY_NAME = CachePolicy.PolicyName.DIVIDE_GR;
                break;
            case "arc":
                CacheParamSetter.POLICY_NAME = CachePolicy.PolicyName.ARC;
                break;
        }
        CacheParamSetter.mCacheSpaceLimit = Long.valueOf(cacheCapacity).toString();
    }

    static void randomTest(AlluxioURI path, int gigasize) {
        long preciseTime = 0;
        FileSystem fs = CacheFileSystem.get(true);

        ClientCacheStatistics.INSTANCE.clear();
        try {
            long readCount = 0;
            FileInStream in = fs.openFile(path);
            for (int i = 0; i < gigasize * 1000; i++) {
                readCount += 1;
                long offset = RandomUtils.nextLong(0, 20L * 1024 * 1024 * 1024 - 4 * 1024 * 1024);
                int size = RandomUtils.nextInt(8 * 1024, 4 * 1024 * 1024);
                byte[] buffer = new byte[size];
                try {
                    long a = System.currentTimeMillis();
                    int readedLen = in.positionedRead(offset, buffer, 0, size);
                    preciseTime += System.currentTimeMillis() - a;
                    if (readedLen == -1) {
                        throw new RuntimeException();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
            in.close();

            System.out.println("Statistics:" + readCount + "\n");
            System.out.println("usedTime: " + preciseTime + "\n");
            collectStats();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void collectStats() throws Exception {
        System.out.println("cacheSpace: " + ClientCacheContext.INSTANCE.getCacheLimit() + "\n");
        System.out.println("blockSize: " + ClientCacheContext.CACHE_SIZE + "\n");
        System.out.println("cachePolicy: " + CacheParamSetter.POLICY_NAME + "\n");

        System.out.println("BHR: " + ClientCacheStatistics.INSTANCE.hitRatio() + "\n");
        System.out.println("bytesHit: " + ClientCacheStatistics.INSTANCE.bytesHit + "\n");
        System.out.println("bytesRead: " + ClientCacheStatistics.INSTANCE.bytesRead + "\n");

        System.out.println(ClientCacheStatistics.INSTANCE.toString() + "\n");
    }
}
