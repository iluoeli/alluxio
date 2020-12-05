package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.client.file.cache.Metric.ClientCacheStatistics;
import alluxio.client.file.cache.Metric.ClientCacheStatistics;
import alluxio.client.file.cache.core.CachePolicy;
import alluxio.client.file.cache.core.ClientCacheContext;
import org.junit.Assert;
import org.junit.Test;

public class ClientCacheStatisticsFixedLengthTest {
    @Test
    public void testFixLengthCacheHitRatio() {
        CacheParamSetter.mode = ClientCacheContext.MODE.EVICT;
        CacheParamSetter.POLICY_NAME = CachePolicy.PolicyName.LRU;
        CacheParamSetter.CACHE_SIZE = 100;

        FileSystem fs = CacheFileSystem.get(true);

        try {
            FileInStream in = fs.openFile(new AlluxioURI("/LICENSE"));
            ClientCacheStatistics.INSTANCE.clear();
            byte[] buf = new byte[100];
            // read [0, 100]
            int nread = in.positionedRead(1000, buf, 0, 100);
            // read [0, 100] again
            nread = in.positionedRead(1000, buf, 0, 100);
            Assert.assertEquals(ClientCacheStatistics.INSTANCE.bytesHit, 100);
            Assert.assertEquals(ClientCacheStatistics.INSTANCE.bytesRead, 200);
            ClientCacheStatistics.INSTANCE.clear();

            // cached unit [0, 100]
            // read [0, 50]; actually read: cache [0, 50], ufs 0;
            in.positionedRead(1000, buf, 0, 50);
            Assert.assertEquals(ClientCacheStatistics.INSTANCE.bytesHit, 50);
            Assert.assertEquals(ClientCacheStatistics.INSTANCE.bytesRead, 50);
            ClientCacheStatistics.INSTANCE.clear();

            // read [100, 150]; actually read: cache 0, ufs [100, 200]
            in.positionedRead(1100, buf, 0, 50);
            Assert.assertEquals(ClientCacheStatistics.INSTANCE.bytesHit, 0);
            Assert.assertEquals(100, ClientCacheStatistics.INSTANCE.bytesRead);

            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
