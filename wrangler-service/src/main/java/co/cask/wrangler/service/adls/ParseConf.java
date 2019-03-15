/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.wrangler.service.adls;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Utility class to parse hdfs-site.xml to get cluster name for jceks path
 */
public class ParseConf {

    public static String getClusterName() throws IOException {
        Configuration conf = new Configuration();
        String clusterName = conf.get("dfs.internal.nameservices");
        if(clusterName == null || clusterName.isEmpty()){
            throw new IOException("issue with hdfs-site.xml");
        }
        return clusterName;
    }
    // jceks://hdfs@mycluster/etc/security/jceks/adls.jceks
    public static String buildJCEKSPath(String clusterName){
        String jceksPath = "jceks://hdfs@" + clusterName + "/etc/security/jceks/adls.jceks";
        return jceksPath;
    }
}
