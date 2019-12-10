/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.service.explorer;

import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.wrangler.PropertyIds;
import co.cask.wrangler.RequestExtractor;
import co.cask.wrangler.SamplingMethod;
import co.cask.wrangler.ServiceUtils;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.sampling.Bernoulli;
import co.cask.wrangler.sampling.Poisson;
import co.cask.wrangler.sampling.Reservoir;
import co.cask.wrangler.service.common.AbstractWranglerService;
import co.cask.wrangler.service.common.Format;
import co.cask.wrangler.service.connections.ConnectionType;
import co.cask.wrangler.utils.ObjectSerDe;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.Location;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.HttpURLConnection;
import java.security.PrivilegedExceptionAction;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import static co.cask.wrangler.ServiceUtils.error;
import static co.cask.wrangler.ServiceUtils.sendJson;

/**
 * A {@link FilesystemExplorer} is a HTTP Service handler for exploring the filesystem.
 * It provides capabilities for listing file(s) and directories. It also provides metadata.
 */
public class FilesystemExplorer extends AbstractWranglerService {
  private static final Logger LOG = LoggerFactory.getLogger(FilesystemExplorer.class);
  private static final Gson gson =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
  private Explorer explorer;
  private static final String COLUMN_NAME = "body";
  private static final int FILE_SIZE = 10 * 1024 * 1024;

  interface CheckFile { 
      Location run() throws IOException, ExplorerException; 
  } 
  
  interface ReadFileBytes { 
    byte[] run() throws IOException; 
  } 

  interface ReadFile { 
      void run() throws IOException; 
  } 
  
  /**
   * Lists the content of the path specified using the {@Location}.
   *
   * @param request HTTP Request Handler
   * @param responder HTTP Response Handler
   * @param path to the location in the filesystem
   * @throws Exception
   */
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("explorer/fs")
  @GET
  public void list(HttpServiceRequest request, HttpServiceResponder responder,
                   @QueryParam("path") String path,
                   @QueryParam("hidden") boolean hidden) throws Exception {

    LOG.debug("List API. HTTP Request headers: {}",
            Arrays.toString(request.getAllHeaders().entrySet().toArray()));
    
    final String impersonatedUser = request.getHeader(PropertyIds.USER_ID);
    
    try {
        UserGroupInformation proxyUgi = getProxyUGI(impersonatedUser);
        Map<String, Object> listing;
        if (proxyUgi == null) {
          listing = explorer.browse(path, hidden);
        } else {
          listing = proxyUgi.doAs(new PrivilegedExceptionAction<Map<String, Object>> () {
            public Map<String, Object> run() throws Exception {
                return explorer.browse(path, hidden);
            }
          });
        }
      sendJson(responder, HttpURLConnection.HTTP_OK, gson.toJson(listing));
    } catch (UndeclaredThrowableException e) {
      LOG.error("Exception stack: {}", e);
      error(responder, e.getUndeclaredThrowable().getMessage());
    } catch (Exception e) {
      LOG.error("Exception stack: {}", e);
      error(responder, e.getMessage());
    }
  }

  /**
   * Given a path, reads the file into the workspace.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param path to the location in the filesystem.
   * @param lines number of lines to extracted from file if it's a text/plain.
   * @param sampler sampling method to be used.
   */
  @Path("explorer/fs/read")
  @GET
  public void read(HttpServiceRequest request, HttpServiceResponder responder,
                   @QueryParam("path") String path, @QueryParam("lines") int lines,
                   @QueryParam("sampler") String sampler,
                   @QueryParam("fraction") double fraction,
                   @QueryParam("scope") String scope) {
      
    LOG.debug("Read API. HTTP Request headers: {}", 
            Arrays.toString(request.getAllHeaders().entrySet().toArray()));
    LOG.debug("Read API. Query Params: Path {}, lines {}, sampler {}, fraction {}, scope {}",
            path, lines, sampler, fraction, scope);

    RequestExtractor extractor = new RequestExtractor(request);
    String header = extractor.getHeader(RequestExtractor.CONTENT_TYPE_HEADER, null);

    if (header == null) {
      error(responder, "Content-Type header not specified.");
      return;
    }

    if (scope == null || scope.isEmpty()) {
      scope = WorkspaceDataset.DEFAULT_SCOPE;
    }
    
    final String impersonatedUser = request.getHeader(PropertyIds.USER_ID);

    try {
      UserGroupInformation proxyUgi = getProxyUGI(impersonatedUser);
   
      if (header.equalsIgnoreCase("text/plain") || header.contains("text/")) {
        loadSamplableFile(responder, scope, path, lines, fraction, sampler,
              proxyUgi, impersonatedUser);
      } else if (header.equalsIgnoreCase("application/xml")) {
        loadFile(responder, scope, path, DataType.RECORDS, proxyUgi, impersonatedUser);
      } else if (header.equalsIgnoreCase("application/json")) {
        loadFile(responder, scope, path, DataType.TEXT, proxyUgi, impersonatedUser);
      } else if (header.equalsIgnoreCase("application/avro")
        || header.equalsIgnoreCase("application/protobuf")
        || header.equalsIgnoreCase("application/excel")
        || header.contains("image/")) {
        loadFile(responder, scope, path, DataType.BINARY, proxyUgi, impersonatedUser);
      } else {
        error(responder, "Currently doesn't support wrangling of this type of file.");
      }
    } catch (UndeclaredThrowableException e) {
      LOG.error("UndeclaredThrowableException stack: {}", e);
      error(responder, e.getUndeclaredThrowable().getMessage());
    } catch (Exception e) {
      LOG.error("Exception stack: {}", e);
      error(responder, e.getMessage());
    }
  }

  /**
   * Specification for the source.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param path to the location in the filesystem.
   */
  @Path("explorer/fs/specification")
  @GET
  public void specification(HttpServiceRequest request, HttpServiceResponder responder,
                            @QueryParam("path") String path, @QueryParam("wid") String workspaceId) {
      
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
      Location location = explorer.getLocation(path);
      JsonObject value = new JsonObject();
      JsonObject file = new JsonObject();
      properties.put("path", location.toURI().toString());
      properties.put("referenceName", location.getName());
      properties.put("ignoreNonExistingFolders", "false");
      properties.put("recursive", "false");
      properties.put("copyHeader", String.valueOf(shouldCopyHeader(workspaceId)));
      properties.put("schema", format.getSchema().toString());
      file.add("properties", gson.toJsonTree(properties));
      file.addProperty("name", "File");
      file.addProperty("type", "source");
      value.add("File", file);
      JsonArray values = new JsonArray();
      values.add(value);
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", values.size());
      response.add("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (Exception e) {
      LOG.error("Exception stack: {}", e);
      error(responder, e.getMessage());
    }
  }

  private void loadFile(HttpServiceResponder responder, String scope, String path, DataType type,
          UserGroupInformation ugi, String impersonatedUser) {
    JsonObject response = new JsonObject();    

    try {
        Location location;
        CheckFile cf = () -> {
          Location loc = explorer.getLocation(path);
          if (!loc.exists()) {
            throw new IOException(String.format("%s (No such file)", path));
          }

          if (loc.length() > FILE_SIZE) {
            throw new IOException("Files larger than 10MB are currently not supported.");
          }
          return loc;
        };
      
       
      if (ugi == null) {
        location = cf.run();
      } else {
        location = ugi.doAs(new PrivilegedExceptionAction<Location> () {
          public Location run() throws Exception {
            return cf.run();
          }
        });
      }
      // Creates workspace.
      String name = location.getName();
      String id = String.format("%s:%s:%s:%d", scope, location.getName(),
                                location.toURI().getPath(), System.nanoTime());
      id = ServiceUtils.generateMD5(id);
      LOG.debug("Creating workspace meta .. id {}, scope {}, name {}", id, scope, name);
      ws.createWorkspaceMeta(id, scope, name);

      byte[] bytes;
      ReadFileBytes rf = () -> {
        byte[] bytesRead = new byte[(int)(location.length()) + 1];
        BufferedInputStream stream = null;
        try {
          stream = new BufferedInputStream(location.getInputStream());
          stream.read(bytesRead);
        } finally {
          if (stream != null) {
            try {
              stream.close();
            } catch (IOException e) {
              // Nothing much we can do here.
            }
          }
        }
        return bytesRead;
      };
      
      if (ugi == null) {
        bytes = rf.run();
      } else {
        bytes = ugi.doAs(new PrivilegedExceptionAction<byte[]> () {
          public byte[] run() throws Exception {
            return rf.run();
          }
        });
      }

      // Set all properties and write to workspace.
      Map<String, String> properties = new HashMap<>();
      properties.put(PropertyIds.FILE_NAME, location.getName());
      properties.put(PropertyIds.URI, location.toURI().toString());
      properties.put(PropertyIds.FILE_PATH, location.toURI().getPath());
      properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.FILE.getType());
      properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
      Format format = type == DataType.BINARY ? Format.BLOB : Format.TEXT;
      properties.put(PropertyIds.FORMAT, format.name());
      ws.writeProperties(id, properties);

      // Write records to workspace.
      if(type == DataType.RECORDS) {
        List<Row> rows = new ArrayList<>();
        rows.add(new Row(COLUMN_NAME, new String(bytes, Charsets.UTF_8)));
        ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
        byte[] data = serDe.toByteArray(rows);
        ws.writeToWorkspace(id, WorkspaceDataset.DATA_COL, DataType.RECORDS, data);
      } else if (type == DataType.BINARY || type == DataType.TEXT) {
        ws.writeToWorkspace(id, WorkspaceDataset.DATA_COL, type, bytes);
      }

      // Preparing return response to include mandatory fields : id and name.
      JsonArray values = new JsonArray();
      JsonObject object = new JsonObject();
      object.addProperty(PropertyIds.ID, id);
      object.addProperty(PropertyIds.NAME, name);
      object.addProperty(PropertyIds.URI, location.toURI().toString());
      object.addProperty(PropertyIds.FILE_PATH, location.toURI().getPath());
      object.addProperty(PropertyIds.FILE_NAME, location.getName());
      object.addProperty(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
      values.add(object);

      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", values.size());
      response.add("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (UndeclaredThrowableException e) {
      LOG.error("UndeclaredThrowableException stack: {}", e);
      error(responder, e.getUndeclaredThrowable().getMessage());
    } catch (Exception e) {
      LOG.error("Exception stack: {}", e);
      error(responder, e.getMessage());
    } 
  }

  private void loadSamplableFile(HttpServiceResponder responder,
                                 String scope, String path, int lines, double fraction, String sampler,
                                 UserGroupInformation ugi, String impersonatedUser) {
    JsonObject response = new JsonObject();
    SamplingMethod samplingMethod = SamplingMethod.fromString(sampler);

    if (sampler == null || sampler.isEmpty() || SamplingMethod.fromString(sampler) == null) {
      samplingMethod = SamplingMethod.FIRST;
    }
    
    final SamplingMethod finalSamplingMethod = samplingMethod;
    
    BoundedLineInputStream stream = null;
    try {
      Location location;
      CheckFile cf = () -> {
        Location loc = explorer.getLocation(path);
        if (!loc.exists()) {
          throw new IOException(String.format("%s (No such file)", path));
        }
        return loc;
      };
        
      if (ugi == null) {
        location= cf.run();
      } else {
        location = ugi.doAs(new PrivilegedExceptionAction<Location> () {
          public Location run() throws Exception {
            return cf.run();
          }
        });
      }

      String name = location.getName();
      String id = String.format("%s:%s:%d", location.getName(), location.toURI().getPath(), System.nanoTime());
      id = ServiceUtils.generateMD5(id);
      LOG.debug("Creating workspace meta .. id {}, scope {}, name {}", id, scope, name);
      ws.createWorkspaceMeta(id, scope, name);

      // Iterate through lines to extract only 'limit' random lines.
      // Depending on the type, the sampling of the input is performed.
      List<Row> rows = new ArrayList<>();
      
      ReadFile rf = () -> {
        BoundedLineInputStream blis = BoundedLineInputStream.iterator(location.getInputStream(),
            Charsets.UTF_8, lines);
        Iterator<String> it = blis;
        if (finalSamplingMethod == SamplingMethod.POISSON) {
          it = new Poisson<String>(fraction).sample(blis);
        } else if (finalSamplingMethod == SamplingMethod.BERNOULLI) {
          it = new Bernoulli<String>(fraction).sample(blis);
        } else if (finalSamplingMethod == SamplingMethod.RESERVOIR) {
          it = new Reservoir<String>(lines).sample(blis);
        }
        while(it.hasNext()) {
          rows.add(new Row(COLUMN_NAME, it.next()));
        }
      };

      if (ugi == null) {
        rf.run();
      } else {
        ugi.doAs(new PrivilegedExceptionAction<Integer> () {
          public Integer run() throws Exception {
            rf.run();
            return 0;
          }
        });
      }
 
      // Set all properties and write to workspace.
      Map<String, String> properties = new HashMap<>();
      properties.put(PropertyIds.FILE_NAME, location.getName());
      properties.put(PropertyIds.URI, location.toURI().toString());
      properties.put(PropertyIds.FILE_PATH, location.toURI().getPath());
      properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.FILE.getType());
      properties.put(PropertyIds.SAMPLER_TYPE, samplingMethod.getMethod());
      ws.writeProperties(id, properties);

      // Write rows to workspace.
      ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
      byte[] data = serDe.toByteArray(rows);
      ws.writeToWorkspace(id, WorkspaceDataset.DATA_COL, DataType.RECORDS, data);

      // Preparing return response to include mandatory fields : id and name.
      JsonArray values = new JsonArray();
      JsonObject object = new JsonObject();
      object.addProperty(PropertyIds.ID, id);
      object.addProperty(PropertyIds.NAME, name);
      object.addProperty(PropertyIds.URI, location.toURI().toString());
      object.addProperty(PropertyIds.FILE_PATH, location.toURI().getPath());
      object.addProperty(PropertyIds.FILE_NAME, location.getName());
      object.addProperty(PropertyIds.SAMPLER_TYPE, samplingMethod.getMethod());
      values.add(object);

      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", values.size());
      response.add("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (ExplorerException e) {
      LOG.error("ExplorerException stack: {}", e);
      error(responder, e.getMessage());
    } catch (IOException e) {
      LOG.error("IOException stack: {}", e);
      error(responder, e.getMessage());
    } catch (UndeclaredThrowableException e) {
      LOG.error("UndeclaredThrowableException stack: {}", e);
      error(responder, e.getUndeclaredThrowable().getMessage());
    } catch (Exception e) {
      LOG.error("Exception stack: {}", e);
      error(responder, e.getMessage());
    } finally {
      if (stream != null) {
        stream.close();
      }
    }
  }

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    final HttpServiceContext ctx = context;
    Security.addProvider(new BouncyCastleProvider());
    this.explorer = new Explorer(new DatasetProvider() {
      @Override
      public Dataset acquire() {
        return ctx.getDataset("dataprepfs");
      }

      @Override
      public void release(Dataset dataset) {
        ctx.discardDataset(dataset);
      }
    });
  }
}
