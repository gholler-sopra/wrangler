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

import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.wrangler.RequestExtractor;
import co.cask.wrangler.ServiceUtils;
import co.cask.wrangler.dataset.connections.Connection;
import co.cask.wrangler.service.FileTypeDetector;
import co.cask.wrangler.service.common.AbstractWranglerService;
import co.cask.wrangler.service.connections.ConnectionType;
import com.google.common.base.Charsets;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.util.List;

import static co.cask.wrangler.ServiceUtils.error;

//import co.cask.wrangler.dataset.workspace.WorkspaceMeta;
//import co.cask.wrangler.proto.connection.Connection;
//import co.cask.wrangler.proto.connection.ConnectionMeta;
//import co.cask.wrangler.proto.connection.ConnectionType;


public class ADLSHandler extends AbstractWranglerService {

    private static final FileTypeDetector detector = new FileTypeDetector();

    /**
     * Tests ADLS Connection.
     *
     * @param request HTTP Request handler.
     * @param responder HTTP Response handler.
     */
//    @POST
//    @Path("/connections/adls/test")
//    public void testADLSConnection(HttpServiceRequest request, HttpServiceResponder responder) {
//        testADLSConnection(request, responder, getContext().getNamespace());
//    }

    @POST
    @Path("connections/adls/test")
    public void testADLSConnection(HttpServiceRequest request, HttpServiceResponder responder) {
        try {
            // Extract the body of the request and transform it to the Connection object.
            RequestExtractor extractor = new RequestExtractor(request);
            Connection connection = extractor.getContent(Charsets.UTF_8.name(), Connection.class);
            ConnectionType connectionType = ConnectionType.fromString(connection.getType().getType());
            if (connectionType == ConnectionType.UNDEFINED || connectionType != ConnectionType.S3) {
                error(responder,
                        String.format("Invalid connection type %s set, expected 'S3' connection type.",
                                connectionType.getType()));
                return;
            }
            // creating a client doesn't test the connection, we will do list buckets so the connection is tested.
            ADLStoreClient client = initializeAndGetADLSClient(connection);
                List<DirectoryEntry> listDirectory = client.enumerateDirectory("/");
                if(listDirectory.size()==0){
                    ServiceUtils.error(responder, "Client is not working");
                }else{
                    ServiceUtils.success(responder, "Success");
                }
        } catch (Exception e) {
            ServiceUtils.error(responder, e.getMessage());
        }
    }

//    public void testADLSConnection(Connection connection) throws IOException {
//        /**
//         *  To check if the client is connected, we will check if a default file /a/b/c.txt exists or not
//         */
//        ADLStoreClient client = initializeAndGetADLSClient(connection);
//        System.out.println(client.checkExists("abc.txt"));
//        try{
//            List<DirectoryEntry> listDirectory = client.enumerateDirectory("/");
//            if(listDirectory.size()==0){
//                System.out.println("The client is not up");
//            }else{
//                System.out.println("The client is working");
//            }
//        } catch (IOException e){
//            System.out.println("Input path may be wrong");
//        }
//    }

    // creates ADLS client and sets region and returns the initialized client
    private ADLStoreClient initializeAndGetADLSClient(Connection connection) {
        ADLSConfiguration ADLSConfiguration = new ADLSConfiguration(connection);
        String authTokenEndpoint = ADLSConfiguration.getEndpointUrl();
        String clientId = ADLSConfiguration.getADLSClientId();
        String clientKey = ADLSConfiguration.getClientKey();
        String accountFQDN = ADLSConfiguration.getAccountFQDN();
        AccessTokenProvider provider = new ClientCredsTokenProvider(authTokenEndpoint, clientId, clientKey);
        ADLStoreClient client = ADLStoreClient.createClient(accountFQDN, provider);
        return client;
    }
}
