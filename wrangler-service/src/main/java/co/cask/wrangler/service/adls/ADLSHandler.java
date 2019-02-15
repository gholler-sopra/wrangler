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

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.ReadOnly;
import co.cask.cdap.api.annotation.ReadWrite;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.wrangler.PropertyIds;
import co.cask.wrangler.RequestExtractor;
import co.cask.wrangler.SamplingMethod;
import co.cask.wrangler.ServiceUtils;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.dataset.connections.Connection;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.sampling.Bernoulli;
import co.cask.wrangler.sampling.Poisson;
import co.cask.wrangler.sampling.Reservoir;
import co.cask.wrangler.service.FileTypeDetector;
import co.cask.wrangler.service.common.AbstractWranglerService;
import co.cask.wrangler.service.common.Format;
import co.cask.wrangler.service.connections.ConnectionType;
import co.cask.wrangler.service.explorer.BoundedLineInputStream;
import co.cask.wrangler.utils.ObjectSerDe;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.gson.*;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;

import javax.ws.rs.*;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.*;

import static co.cask.wrangler.ServiceUtils.error;
import static co.cask.wrangler.ServiceUtils.sendJson;

public class ADLSHandler extends AbstractWranglerService {
    private static final Gson gson =
            new GsonBuilder().
                    setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_DASHES).
                    registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
    private static final String COLUMN_NAME = "body";
    private static final int FILE_SIZE = 10 * 1024 * 1024;
    private static final FileTypeDetector detector = new FileTypeDetector();

    /**
     * Tests ADLS Connection.
     *
     * @param request HTTP Request handler.
     * @param responder HTTP Response handler.
     */
    @POST
    @Path("connections/adls/test")
    public void testADLSConnection(HttpServiceRequest request, HttpServiceResponder responder) {
        try {
            // Extract the body of the request and transform it to the Connection object.
            RequestExtractor extractor = new RequestExtractor(request);
            Connection connection = extractor.getContent(Charsets.UTF_8.name(), Connection.class);
            ConnectionType connectionType = ConnectionType.fromString(connection.getType().getType());
            if (connectionType == ConnectionType.UNDEFINED || connectionType != ConnectionType.ADLS) {
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

    // creates ADLS client and returns the initialized client
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

    private boolean validateConnection(String connectionId, Connection connection, HttpServiceResponder responder) {
        if (connection == null) {
            error(responder, "Unable to find connection in store for the connection id - " + connectionId);
            return false;
        }
        if (ConnectionType.ADLS != connection.getType()) {
            error(responder, "Invalid connection type set, this endpoint only accepts S3 connection type");
            return false;
        }
        return true;
    }

    /**
     * Lists ADLS directory's contents for the given prefix path.
     * @param request HTTP Request handler.
     * @param responder HTTP Response handler.
     */
    @TransactionPolicy(value = TransactionControl.EXPLICIT)
    @ReadOnly
    @GET
    @Path("/connections/{connection-id}/adls/explore")
    public void listADLSDirectoryInfo(HttpServiceRequest request, HttpServiceResponder responder,
                                @PathParam("connection-id") final String connectionId,
                                @QueryParam("path") String path) throws IOException {
        try {
            final Connection[] connection = new Connection[1];
            getContext().execute(new TxRunnable() {
                @Override
                public void run(DatasetContext datasetContext) throws Exception {
                    connection[0] = store.get(connectionId);
                }
            });
            if (!validateConnection(connectionId, connection[0], responder)) {
                return;
            }


            ADLStoreClient adlStoreClient = initializeAndGetADLSClient(connection[0]);
            if(!adlStoreClient.checkExists(path)){
                ServiceUtils.error(responder, "Client doesn't exist");
            }
            List<DirectoryEntry> list = adlStoreClient.enumerateDirectory(path);
            JsonArray values = new JsonArray();
            for (DirectoryEntry entry : list) {
                JsonObject object = new JsonObject();
                object.addProperty("Name", entry.name);
                object.addProperty("Fullname", entry.fullName);
                object.addProperty("Length", entry.length);
                object.addProperty("Type", entry.type.toString());
                object.addProperty("Group", entry.group);
                object.addProperty("User",entry.user);
                object.addProperty("Permission",entry.permission);
                object.addProperty("Modified time",entry.lastModifiedTime.toString());
                object.addProperty("Last Access Time",entry.lastAccessTime.toString());
                values.add(object);
            }
            JsonObject response = new JsonObject();
            response.add("values", values);
            sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
        } catch (Exception e) {
            ServiceUtils.error(responder, e.getMessage());
        }
    }

    // Load files in ADLS into workspace
    /**
     * Reads ADLS file into workspace
     * @param request HTTP Request handler.
     * @param responder HTTP Response handler.
     */
    @POST
    @ReadWrite
    @Path("/connections/{connection-id}/adls/read")
    public void loadADLSFile(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("connection-id") String connectionId,
                           @QueryParam("file-path") String filePath, @QueryParam("lines") int lines,
                           @QueryParam("sampler") String sampler, @QueryParam("fraction") double fraction,
                           @QueryParam("scope") String scope) {
        try {
            if (Strings.isNullOrEmpty(connectionId)) {
                responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Required path param 'connection-id' is missing in the input");
                return;
            }

            if (Strings.isNullOrEmpty(scope)) {
                scope = WorkspaceDataset.DEFAULT_SCOPE;
            }

            RequestExtractor extractor = new RequestExtractor(request);
            String header = extractor.getHeader(RequestExtractor.CONTENT_TYPE_HEADER, null);
            Connection connection = store.get(connectionId);
            if (!validateConnection(connectionId, connection, responder)) {
                return;
            }
            ADLStoreClient client = initializeAndGetADLSClient(connection);
            DirectoryEntry file =  client.getDirectoryEntry(filePath);
            if (file != null) {
                try (InputStream inputStream = client.getReadStream(file.fullName)) {
                    if (header != null && header.equalsIgnoreCase("text/plain")) {
                        loadSamplableFile(connection.getId(), responder, scope, inputStream, file, lines, fraction, sampler);
                        return;
                    }
                    loadFile(connection.getId(), responder, inputStream, file);
                }
            } else {
                ServiceUtils.error(responder,
                        String.format("ADLS file with name %s is not found", file.fullName));
                return;
            }
        }  catch (Exception e) {
            ServiceUtils.error(responder, e.getMessage());
        }
    }

    private void loadSamplableFile(String connectionId, HttpServiceResponder responder,
                                   String scope, InputStream inputStream, DirectoryEntry fileEntry,
                                   int lines, double fraction, String sampler) {
        JsonObject response = new JsonObject();
        SamplingMethod samplingMethod = SamplingMethod.fromString(sampler);
        if (sampler == null || sampler.isEmpty() || SamplingMethod.fromString(sampler) == null) {
            samplingMethod = SamplingMethod.FIRST;
        }

        try(BoundedLineInputStream blis = BoundedLineInputStream.iterator(inputStream, Charsets.UTF_8, lines)) {
            String name = fileEntry.name;

            String file = String.format("%s:%s:%s", scope, fileEntry.name, fileEntry.fullName);
            String fileName = fileEntry.fullName;
            String identifier = ServiceUtils.generateMD5(file);
            ws.createWorkspaceMeta(identifier, scope, fileName);

            // Iterate through lines to extract only 'limit' random lines.
            // Depending on the type, the sampling of the input is performed.
            List<Row> rows = new ArrayList<>();
            Iterator<String> it = blis;
            if (samplingMethod == SamplingMethod.POISSON) {
                it = new Poisson<String>(fraction).sample(blis);
            } else if (samplingMethod == SamplingMethod.BERNOULLI) {
                it = new Bernoulli<String>(fraction).sample(blis);
            } else if (samplingMethod == SamplingMethod.RESERVOIR) {
                it = new Reservoir<String>(lines).sample(blis);
            }
            while(it.hasNext()) {
                rows.add(new Row(COLUMN_NAME, it.next()));
            }

            // Set all properties and write to workspace.
            Map<String, String> properties = new HashMap<>();
            properties.put(PropertyIds.ID, identifier);
            properties.put(PropertyIds.NAME, fileName);
            properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.ADLS.getType());
            properties.put(PropertyIds.SAMPLER_TYPE, samplingMethod.getMethod());
            properties.put(PropertyIds.CONNECTION_ID, connectionId);
            // ADLS specific properties.
            properties.put("file-name", fileEntry.fullName);
            ws.writeProperties(identifier, properties);

            // Write rows to workspace.
            ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
            byte[] data = serDe.toByteArray(rows);
            ws.writeToWorkspace(identifier, WorkspaceDataset.DATA_COL, DataType.RECORDS, data);

            // Preparing return response to include mandatory fields : id and name.
            JsonArray values = new JsonArray();
            JsonObject object = new JsonObject();
            object.addProperty(PropertyIds.ID, identifier);
            object.addProperty(PropertyIds.NAME, name);
            object.addProperty(PropertyIds.CONNECTION_TYPE, ConnectionType.ADLS.getType());
            object.addProperty(PropertyIds.SAMPLER_TYPE, samplingMethod.getMethod());
            object.addProperty(PropertyIds.CONNECTION_ID, connectionId);
            object.addProperty("file-name", fileEntry.fullName);
            values.add(object);

            response.addProperty("status", HttpURLConnection.HTTP_OK);
            response.addProperty("message", "Success");
            response.addProperty("count", values.size());
            response.add("values", values);
            sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
        } catch (IOException e) {
            error(responder, e.getMessage());
        } catch (Exception e) {
            error(responder, e.getMessage());
        }
    }

    private void loadFile(String connectionId, HttpServiceResponder responder, InputStream inputStream, DirectoryEntry fileEntry) {
        JsonObject response = new JsonObject();
        BufferedInputStream stream = null;
        try {

            if (fileEntry.length > FILE_SIZE) {
                error(responder, "Files greater than 10MB are not supported.");
                return;
            }

            // Creates workspace.
            String name = fileEntry.name;

            String file = String.format("%s:%s", name, fileEntry.fullName);
            String identifier = ServiceUtils.generateMD5(file);
            String fileName = fileEntry.fullName;
            ws.createWorkspaceMeta(identifier, fileName);

            stream = new BufferedInputStream(inputStream);
            byte[] bytes = new byte[(int) fileEntry.length + 1];
            stream.read(bytes);

            // Set all properties and write to workspace.
            Map<String, String> properties = new HashMap<>();
            properties.put(PropertyIds.ID, identifier);
            properties.put(PropertyIds.NAME, fileName);
            properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.ADLS.getType());
            properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
            properties.put(PropertyIds.CONNECTION_ID, connectionId);
            DataType dataType = getDataType(name);
            Format format = dataType == DataType.BINARY ? Format.BLOB : Format.TEXT;
            properties.put(PropertyIds.FORMAT, format.name());

            // S3 specific properties.
            properties.put("file-name", fileEntry.fullName);
            ws.writeProperties(identifier, properties);
            ws.writeToWorkspace(identifier, WorkspaceDataset.DATA_COL, getDataType(name), bytes);

            // Preparing return response to include mandatory fields : id and name.
            JsonArray values = new JsonArray();
            JsonObject object = new JsonObject();
            object.addProperty(PropertyIds.ID, identifier);
            object.addProperty(PropertyIds.NAME, name);
            object.addProperty(PropertyIds.CONNECTION_TYPE, ConnectionType.ADLS.getType());
            object.addProperty(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
            object.addProperty(PropertyIds.CONNECTION_ID, connectionId);
            object.addProperty("file-name", fileEntry.fullName);
            values.add(object);

            response.addProperty("status", HttpURLConnection.HTTP_OK);
            response.addProperty("message", "Success");
            response.addProperty("count", values.size());
            response.add("values", values);
            sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
        } catch (Exception e) {
            error(responder, e.getMessage());
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    // Nothing much we can do here.
                }
            }
        }
    }

    /**
     * Specification for the source.
     *
     * @param request HTTP request handler.
     * @param responder HTTP response handler.
     */
    @Path("/connections/{connection-id}/adls/specification")
    @GET
    public void specification(HttpServiceRequest request, final HttpServiceResponder responder,
                              @PathParam("connection-id") String connectionId,
                              @QueryParam("wid") String workspaceId) {
        JsonObject response = new JsonObject();
        try {
            Format format = Format.TEXT;
            if (workspaceId != null) {
                Map<String, String> config = ws.getProperties(workspaceId);
                String formatStr = config.getOrDefault(PropertyIds.FORMAT, Format.TEXT.name());
                format = Format.valueOf(formatStr);
            }
            Map<String, String> properties = new HashMap<>();
            properties.put("format", format.name().toLowerCase());
            Connection conn = store.get(connectionId);
            ADLSConfiguration adlsConfiguration = new ADLSConfiguration(conn);
            JsonObject value = new JsonObject();
            JsonObject adls = new JsonObject();
            properties.put("accountFQDN", adlsConfiguration.getAccountFQDN());
            properties.put("input-directory", adlsConfiguration.getDirectory());
            properties.put("client-key", adlsConfiguration.getClientKey());
            properties.put("client-id",adlsConfiguration.getADLSClientId());
            properties.put("copyHeader", String.valueOf(shouldCopyHeader(workspaceId)));
            properties.put("schema", format.getSchema().toString());

            adls.add("properties", gson.toJsonTree(properties));
            adls.addProperty("name", "ADLS");
            adls.addProperty("type", "source");
            value.add("adls", adls);

            JsonArray values = new JsonArray();
            values.add(value);
            response.addProperty("status", HttpURLConnection.HTTP_OK);
            response.addProperty("message", "Success");
            response.addProperty("count", values.size());
            response.add("values", values);
            sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
        } catch (Exception e) {
            error(responder, e.getMessage());
        }
    }

    /**
     * get data type from the file type.
     * @param fileName
     * @return DataType
     * @throws IOException
     */
    private DataType getDataType(String fileName) throws IOException {
        // detect fileType from fileName
        String fileType = detector.detectFileType(fileName);
        DataType dataType = DataType.fromString(fileType);
        return dataType == null ? DataType.BINARY : dataType;
    }

}
