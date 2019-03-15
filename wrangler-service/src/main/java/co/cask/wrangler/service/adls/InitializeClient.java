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
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;

public class InitializeClient {

    public static ADLStoreClient initializeAndGetADLSClient(Connection connection) {
        ADLSConfiguration ADLSConfiguration = new ADLSConfiguration(connection);
        String authTokenEndpoint = ADLSConfiguration.getEndpointURL();
        String clientId = ADLSConfiguration.getADLSClientId();
        String clientKey = ADLSConfiguration.getClientKey();
        String accountFQDN = ADLSConfiguration.getAccountFQDN();
        AccessTokenProvider provider = new ClientCredsTokenProvider(authTokenEndpoint, clientId, clientKey);
        return ADLStoreClient.createClient(accountFQDN, provider);
    }

}
