/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file;

import alluxio.client.file.cache.core.CachePolicy;
import alluxio.client.file.cache.core.ClientCacheContext;

public class CacheParamSetter {
    public static String mCacheSpaceLimit = "1g";
    public static int CACHE_SIZE = 1048576;
    public static CachePolicy.PolicyName POLICY_NAME = CachePolicy.PolicyName.ISK;
    public static ClientCacheContext.MODE mode = ClientCacheContext.MODE.PROMOTE;

    // copied from ClientCacheContext
    public static long getSpaceLimit() {
        String num = mCacheSpaceLimit.substring(0, mCacheSpaceLimit.length() - 1);
        char unit = mCacheSpaceLimit.charAt(mCacheSpaceLimit.length() - 1);
        double n = Double.parseDouble(num);
        if (unit == 'M' || unit == 'm') {
            return (long) (n * 1024 * 1024);
        }
        if (unit == 'K' || unit == 'k') {
            return (long) (n * 1024);
        }
        if (unit == 'G' || unit == 'g') {
            return (long) (n * 1024 * 1024 * 1024);
        }
        return (long) n;
    }
}
