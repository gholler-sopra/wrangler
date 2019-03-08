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
    private static final List<String> CONFIG_FIELDS = ImmutableList.of("clientID", "clientSecret","refreshURL","accountFQDN");
    private static final List<String> CONFIG_FIELDS_JCEKS = ImmutableList.of("kvURL","clientIDKey","clientSecretKey","endPointURLKey","accountFQDN");
    private final String kvURL;
    private String clientIDKey;
    private String clientSecretKey;
    private String endPointURLKey;
    private final String clientID;
    private final String clientSecret;
    private final String refreshURL;
    private final String accountFQDN;

    ADLSConfiguration(Connection connection){
        Map<String, String> properties = connection.getAllProps();

        if (properties == null || properties.size() == 0) {
            throw new IllegalArgumentException("ADLS properties are not defined. Check connection setting.");
        }

        if (properties.containsKey(CONFIG_FIELDS_JCEKS.get(0)) || properties.containsKey(CONFIG_FIELDS_JCEKS.get(1))){
            for (String property : CONFIG_FIELDS_JCEKS) {

                if (!properties.containsKey(property)) {
                    throw new IllegalArgumentException("Missing configuration in connection for property " + property);
                }
            }
            kvURL = properties.get(CONFIG_FIELDS_JCEKS.get(0));
            clientIDKey = properties.get(CONFIG_FIELDS_JCEKS.get(1));
            clientSecretKey = properties.get(CONFIG_FIELDS_JCEKS.get(2));
            endPointURLKey = properties.get(CONFIG_FIELDS_JCEKS.get(3));
            accountFQDN = properties.get(CONFIG_FIELDS_JCEKS.get(4));

            Map<String, String> credentials = AzureClientSecretService.getADLSSecretsUsingJceksAndKV(kvURL, getKvKeyNamesMap(clientIDKey,clientSecretKey,endPointURLKey));

            this.refreshURL = credentials.get(endPointURLKey);
            this.clientID = credentials.get(clientIDKey);
            this.clientSecret = credentials.get(clientSecretKey);

        } else if(properties.containsKey(CONFIG_FIELDS.get(0))){
            for (String property : CONFIG_FIELDS) {

                if (!properties.containsKey(property)) {
                    throw new IllegalArgumentException("Missing configuration in connection for property " + property);
                }
            }
            kvURL = null;
            clientID = properties.get(CONFIG_FIELDS.get(0));
            clientSecret = properties.get(CONFIG_FIELDS.get(1));
            refreshURL = properties.get(CONFIG_FIELDS.get(2));
            accountFQDN = properties.get(CONFIG_FIELDS.get(3));
        } else{
            throw new IllegalArgumentException("Check configuration properties");
        }

    }

    public HashMap<String, String> getKvKeyNamesMap(String clientIDKey, String clientSecretKey, String endPointURLKey) {
        HashMap<String, String> credMap = new HashMap<String, String>();
        credMap.put(clientIDKey,"clientId");
        credMap.put(clientSecretKey,"clientSecret");
        credMap.put(endPointURLKey,"endpointUrl");
        return credMap;
    }

    public String getADLSClientId() {
        return clientID;
    }

    public String getClientIDKey(){ return clientIDKey;}

    public String getClientSecretKey(){return clientSecretKey;}

    public String getEndPointURLKey(){return endPointURLKey;}

    public String getClientKey() {
        return clientSecret;
    }

    public String getEndpointURL() {
        return refreshURL;
    }

    public String getAccountFQDN(){
        return accountFQDN;
    }
}

