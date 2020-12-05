package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.client.file.cache.Metric.ClientCacheStatistics;
import alluxio.client.file.cache.core.CachePolicy;
import alluxio.client.file.cache.core.ClientCacheContext;
import org.junit.Assert;
import org.junit.Test;

public class ClientCacheStatisticsVariableTest {
    @Test
    public void testVariableCacheHitRatio() {
        CacheParamSetter.mode = ClientCacheContext.MODE.EVICT;
        CacheParamSetter.POLICY_NAME = CachePolicy.PolicyName.ISK;

        FileSystem fs = CacheFileSystem.get(true);

        try {
            FileInStream in = fs.openFile(new AlluxioURI("/LICENSE"));
            ClientCacheStatistics.INSTANCE.clear();
            byte[] buf = new byte[100];
            // read [0, 100]
            int nread = in.positionedRead(0, buf, 0, 100);
            // read [50, 150] again
            nread = in.positionedRead(50, buf, 0, 100);

            Assert.assertEquals(ClientCacheStatistics.INSTANCE.bytesHit, 50);
            Assert.assertEquals(ClientCacheStatistics.INSTANCE.bytesRead, 200);

            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
