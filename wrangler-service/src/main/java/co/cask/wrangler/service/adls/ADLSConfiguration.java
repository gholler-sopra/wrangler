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

import co.cask.wrangler.dataset.connections.Connection;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ADLSConfiguration {
    private static final List<String> CONFIG_FIELDS = ImmutableList.of("KVUrl", "JcekPath","ADLS_Directory");
    private final String KVUrl;
    private final String JcekPath;
    private final String ADLS_Directory;
    private final String clientId;
    private final String clientSecret;
    private final String endpointUrl;
    private final String accountFQDN;
    private final String directory;
    private final String kvKeyNames = "clientId:fs.adl.oauth2.client.id,clientSecret:fs.adl.oauth2.credential,endpointUrl:dfs.adls.oauth2.refresh.url";

    ADLSConfiguration(Connection connection){
        Map<String, String> properties = connection.getAllProps();

        if (properties == null || properties.size() == 0) {
            throw new IllegalArgumentException("ADLS properties are not defined. Check connection setting.");
        }

        for (String property : CONFIG_FIELDS) {
            if (!properties.containsKey(property)) {
                throw new IllegalArgumentException("Missing configuration in connection for property " + property);
            }
        }
        KVUrl = properties.get("KVUrl");
        JcekPath = properties.get("JcekPath");
        ADLS_Directory = properties.get("ADLS_Directory");

        Map<String, String> credentials = AzureClientSecretService.getADLSSecretsUsingJceksAndKV(KVUrl, getKvKeyNamesMap(kvKeyNames), JcekPath);

        this.endpointUrl = credentials.get("dfs.adls.oauth2.refresh.url");
        this.clientId = credentials.get("fs.adl.oauth2.client.id");
        this.clientSecret = credentials.get("fs.adl.oauth2.credential");

        String[] Domain = ADLS_Directory.split("/");

        if(Domain.length < 3)
        {
            throw new IllegalArgumentException("Invalid ADLS_DIRECTORY");
        }
        this.accountFQDN = Domain[2];
        System.out.println(accountFQDN);
        String tempDirectory = "/";

        if(Domain.length > 3) {
            for (int i = 3; i < Domain.length; i++) {
                tempDirectory = tempDirectory + Domain[i] + "/";
            }
        }
        tempDirectory = tempDirectory.substring(0, tempDirectory.length() - 1);
        this.directory = tempDirectory;


    }

    public HashMap<String, String> getKvKeyNamesMap(String kvKeyNames) {
        HashMap<String, String> credMap = new HashMap<String, String>();
        String[] keypairs = kvKeyNames.split(",");
        for (String k : keypairs) {
            credMap.put(k.split(":")[1], k.split(":")[0]);
        }
        return credMap;
    }

    public String getADLSClientId() {
        return clientId;
    }

    public String getClientKey() {
        return clientSecret;
    }

    public String getEndpointUrl() {
        return endpointUrl;
    }

    public String getAccountFQDN(){
        return accountFQDN;
    }

    public String getDirectory(){
        return directory;
    }

}

