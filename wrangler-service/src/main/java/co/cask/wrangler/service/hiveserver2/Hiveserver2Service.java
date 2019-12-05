 /*
  * Copyright © 2017-2018 Cask Data, Inc.
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

 package co.cask.wrangler.service.hiveserver2;

 import co.cask.cdap.api.annotation.UseDataSet;

 import co.cask.cdap.api.data.schema.Schema;
 import co.cask.cdap.api.dataset.table.Table;
 import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
 import co.cask.cdap.api.service.http.HttpServiceContext;
 import co.cask.cdap.api.service.http.HttpServiceRequest;
 import co.cask.cdap.api.service.http.HttpServiceResponder;
 import co.cask.cdap.internal.io.SchemaTypeAdapter;
 import co.cask.wrangler.*;
 import co.cask.wrangler.api.Row;
 import co.cask.wrangler.dataset.connections.Connection;
 import co.cask.wrangler.dataset.connections.ConnectionStore;
 import co.cask.wrangler.dataset.workspace.DataType;
 import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
 import co.cask.wrangler.service.connections.ConnectionType;
 import co.cask.wrangler.utils.ObjectSerDe;

 import com.google.common.annotations.VisibleForTesting;
 import com.google.common.base.Strings;
 import com.google.gson.*;

 import org.mortbay.log.Log;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;

 import javax.security.auth.login.LoginException;
 import javax.ws.rs.*;
 import java.io.IOException;
 import java.net.HttpURLConnection;
 import java.sql.Date;
 import java.sql.*;
 import java.time.ZoneId;
 import java.time.ZoneOffset;
 import java.util.*;

 import static co.cask.wrangler.ServiceUtils.error;
 import static co.cask.wrangler.ServiceUtils.sendJson;
 import static co.cask.wrangler.service.directive.DirectivesService.WORKSPACE_DATASET;

 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.security.Credentials;
 import org.apache.hadoop.security.UserGroupInformation;
 import org.apache.hadoop.security.token.Token;

 import org.apache.hadoop.hive.shims.Utils;
 import org.apache.hive.service.auth.HiveAuthFactory;

 /**
  * Class description here.
  */
 public class Hiveserver2Service extends AbstractHttpServiceHandler {

   private static final Logger LOG = LoggerFactory.getLogger(Hiveserver2Service.class);

   private static final String HIVESERVER2 = "hiveserver2";

   public static final String ENABLE_USER_IMPERSONATION_CONFIG_KEY = "system.user.impersonation.enabled";
   public static final String HIVE_ENABLE_ADDING_SCHEMA = "system.append.hive.schema.enabled";

   public static final String HIVE_REMOVE_TABLENAME_FROM_COLUMN_NAME_CONFIG_KEY = "hive.resultset.use.unique.column.names";

   public static final String USER_ID = "CDAP-UserId";

   @UseDataSet(WORKSPACE_DATASET)
   private WorkspaceDataset ws;

   // Data Prep store which stores all the information associated with dataprep.
   @UseDataSet(DataPrep.CONNECTIONS_DATASET)
   private Table table;

   // Abstraction over the table defined above for managing connections.
   private ConnectionStore store;

   private static final Gson gson = new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();

   public interface Executor {
     public void execute(java.sql.Connection connection) throws Exception;
   }

   @Override
   public void initialize(HttpServiceContext context) throws Exception {
     super.initialize(context);
     store = new ConnectionStore(table);
   }

   /**
    * Tests the connection.
    *
    * Following is the response when the connection is successfull.
    *
    * {
    *   "status" : 200,
    *   "message" : "Successfully connected to database."
    * }
    *
    * @param request HTTP request handler.
    * @param responder HTTP response handler.
    */
   @POST
   @Path("connections/hiveserver2/test")
   public void testConnection(HttpServiceRequest request, final HttpServiceResponder responder) {
     java.sql.Connection hiveConnection=null;
     try {
       RequestExtractor extractor = new RequestExtractor(request);
       Connection connection = extractor.getContent("utf-8", Connection.class);

       if (ConnectionType.fromString(connection.getType().getType()) != ConnectionType.HIVESERVER2) {
         error(responder, "Invalid connection type set.");
         return;
       }

       String updatedConnectionURL = addDelegationTokenUpdateURL(connection.getProp("url"), request);
       hiveConnection = DriverManager.getConnection(updatedConnectionURL);
       JsonObject response = new JsonObject();
       response.addProperty("status", HttpURLConnection.HTTP_OK);
       response.addProperty("message", "Successfully connected to hiveserver2.");
       sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());

     } catch (Exception e) {
       error(responder, e.getMessage());
     } finally {
       close(hiveConnection);
     }
   }

   /**
    * Lists all databases.
    *
    * @param request HTTP requets handler.
    * @param responder HTTP response handler.
    */
   @POST
   @Path("connections/hiveserver2/databases")
   public void listDatabases(HttpServiceRequest request, final HttpServiceResponder responder) {
     java.sql.Connection hiveConnection=null;
     PreparedStatement ps = null;
     ResultSet rs = null;
     try {
       RequestExtractor extractor = new RequestExtractor(request);
       Connection connection = extractor.getContent("utf-8", Connection.class);

       String updatedConnectionURL = addDelegationTokenUpdateURL(connection.getProp("url"), request);
       hiveConnection = DriverManager.getConnection(updatedConnectionURL);
       ps = hiveConnection.prepareStatement("show databases");
       rs = ps.executeQuery();

       final JsonArray values = new JsonArray();
       while (rs.next()) {
         values.add(new JsonPrimitive(rs.getString("database_name")));
       }

       final JsonObject response = new JsonObject();
       response.addProperty("status", HttpURLConnection.HTTP_OK);
       response.addProperty("message", "Success.");
       response.addProperty("count", values.size());
       response.add("values", values);
       sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());

     } catch (Exception e) {
       LOG.error("Error occurred while getting database list ", e);
       error(responder, e.getMessage());
     } finally {
       close(rs);
       close(ps);
       close(hiveConnection);
     }
   }

   /**
    * Lists all the tables within a database.
    *
    * @param request HTTP requets handler.
    * @param responder HTTP response handler.
    * @param id Connection id for which the tables need to be listed from database.
    */
   @GET
   @Path("connections/{id}/hiveserver2/tables")
   public void listTables(HttpServiceRequest request, final HttpServiceResponder responder,
                          @PathParam("id") String id) {
     java.sql.Connection hiveConnection=null;
     PreparedStatement ps = null;
     ResultSet rs = null;
     try {
       Connection connection = store.get(id);

       String updatedConnectionURL = addDelegationTokenUpdateURL(connection.getProp("url"), request);
       String hiveDBname = connection.getProp("database");

       String showTablesQuery  = String.format("show tables in %s", hiveDBname);
       hiveConnection = DriverManager.getConnection(updatedConnectionURL);
       ps = hiveConnection.prepareStatement(showTablesQuery);
       rs = ps.executeQuery();

       final JsonArray values = new JsonArray();
       while (rs.next()) {
         String name;
         name = rs.getString(1);
         JsonObject object = new JsonObject();
         object.addProperty("name", name);
         // TODO: For compatibility. Please remove after 4.2, after the UI is fixed.
         object.addProperty("count", 0);
         values.add(object);
       }

       final JsonObject response = new JsonObject();
       response.addProperty("status", HttpURLConnection.HTTP_OK);
       response.addProperty("message", "Success");
       response.addProperty("count", values.size());
       response.add("values", values);
       sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());

     } catch (Exception e) {
       LOG.error("Error occurred while getting table list ", e);
       error(responder, e.getMessage());
     } finally {
       close(rs);
       close(ps);
       close(hiveConnection);
     }
   }

   /**
    * Reads a table into workspace.
    *
    * @param request HTTP requets handler.
    * @param responder HTTP response handler.
    * @param id Connection id for which the tables need to be listed from database.
    * @param table Name of the database table.
    * @param lines No of lines to be read from RDBMS table.
    * @param scope Group the workspace should be created in.
    */
   @GET
   @Path("connections/{id}/hiveserver2/tables/{table}/read")
   public void read(HttpServiceRequest request, final HttpServiceResponder responder,
                    @PathParam("id") final String id, @PathParam("table") final String table,
                    @QueryParam("lines") final int lines, @QueryParam("scope") final String scope) {
     java.sql.Connection hiveConnection=null;
     PreparedStatement ps = null;
     ResultSet rs = null;
     try {
       Connection connection = store.get(id);
       String grp = scope;
       if (Strings.isNullOrEmpty(scope)) {
         grp = WorkspaceDataset.DEFAULT_SCOPE;
       }

       String updatedConnectionURL = addDelegationTokenUpdateURL(connection.getProp("url"), request);
       String dbname = connection.getProp("database");

       hiveConnection = DriverManager.getConnection(updatedConnectionURL);
       ps = hiveConnection.prepareStatement(String.format("select * from %s.%s limit %s", dbname, table, lines));
       rs = ps.executeQuery();
       List<Row> rows = getRows(lines, rs);

       String identifier = ServiceUtils.generateMD5(table);
       ws.createWorkspaceMeta(identifier, grp, table);
       ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
       byte[] data = serDe.toByteArray(rows);
       ws.writeToWorkspace(identifier, WorkspaceDataset.DATA_COL, DataType.RECORDS, data);

       Map<String, String> properties = new HashMap<>();
       properties.put(PropertyIds.ID, identifier);
       properties.put(PropertyIds.NAME, table);
       properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.HIVESERVER2.getType());
       properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
       properties.put(PropertyIds.CONNECTION_ID, id);
       ws.writeProperties(identifier, properties);

       JsonArray values = new JsonArray();
       JsonObject object = new JsonObject();
       object.addProperty(PropertyIds.ID, identifier);
       object.addProperty(PropertyIds.NAME, table);
       object.addProperty(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
       values.add(object);

       final JsonObject response = new JsonObject();
       response.addProperty("status", HttpURLConnection.HTTP_OK);
       response.addProperty("message", "Success");
       response.addProperty("count", values.size());
       response.add("values", values);
       sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());

     } catch (Exception e) {
       LOG.error("Error occurred while closing connection ", e);
       error(responder, e.getMessage());
     } finally {
       close(rs);
       close(ps);
       close(hiveConnection);
     }
   }

   @VisibleForTesting
   public static List<Row> getRows(int lines, ResultSet result) throws SQLException {
     List<Row> rows = new ArrayList<>();
     ResultSetMetaData meta = result.getMetaData();
     int count = lines;
     while (result.next() && count > 0) {
       Row row = new Row();
       for (int i = 1; i < meta.getColumnCount() + 1; ++i) {
         Object object = result.getObject(i);
         if (object != null) {
           if (object instanceof Date) {
             object = ((Date) object).toLocalDate();
           } else if (object instanceof Time) {
             object = ((Time) object).toLocalTime();
           } else if (object instanceof Timestamp) {
             object = ((Timestamp) object).toInstant().atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC));
           }
         }
         row.add(meta.getColumnName(i), object);
       }
       rows.add(row);
       count--;
     }
     return rows;
   }

   /**
    * Specification for the source.
    *
    * @param request HTTP request handler.
    * @param responder HTTP response handler.
    * @param id of the connection.
    * @param table in the database.
    */
   @Path("connections/{id}/hiveserver2/tables/{table}/specification")
   @GET
   public void specification(HttpServiceRequest request, final HttpServiceResponder responder,
                             @PathParam("id") String id, @PathParam("table") final String table) {
     try {
       Connection conn = store.get(id);

       Map<String, String> fileProperties = new HashMap<>();
       fileProperties.put("format", "text");
       fileProperties.put("filenameOnly", "false");
       fileProperties.put("recursive", "false");
       fileProperties.put("referenceName", "dummyfile");

       JsonObject file = new JsonObject();
       file.add("properties", gson.toJsonTree(fileProperties));
       file.addProperty("type", "batchsource");
       file.addProperty("name", "File");

       JsonObject value = new JsonObject();
       JsonObject hiveDatabase = new JsonObject();

       Map<String, String> properties = new HashMap<>();
       properties.put("hivehost", conn.getProp("url"));
       properties.put("referenceName", table);

       properties.put("databaseName", conn.getProp("database"));
       properties.put("tableName", table);
       properties.put("colsToSelect", "*");

       JsonObject schema = getHiveTableSchema(request, conn.getProp("url"), conn.getProp("database"), table);
       if(schema!=null){
         properties.put("schema", schema.toString());
       }

       hiveDatabase.add("properties", gson.toJsonTree(properties));
       hiveDatabase.addProperty("name", "SparkHiveComputeSource");
       hiveDatabase.addProperty("type", "sparkcompute");

       value.add("core-plugins", file);
       value.add("hivesource", hiveDatabase);
       JsonArray values = new JsonArray();
       values.add(value);

       JsonObject response = new JsonObject();
       response.addProperty("status", HttpURLConnection.HTTP_OK);
       response.addProperty("message", "Success");
       response.addProperty("count", values.size());
       response.add("values", values);
       sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
     } catch (Exception e) {
       LOG.error("Error occurred while closing connection ", e);
       error(responder, e.getMessage());
     }
   }

   private JsonObject getHiveTableSchema(HttpServiceRequest request, String connectionURL, String dbName, String tableName){
     JsonObject schema = null;

     if(getContext().getRuntimeArguments().containsKey(HIVE_ENABLE_ADDING_SCHEMA)
             && getContext().getRuntimeArguments().get(HIVE_ENABLE_ADDING_SCHEMA).equalsIgnoreCase("true")) {

       java.sql.Connection hiveConnection = null;
       PreparedStatement ps = null;
       ResultSet rs = null;
       try {
         String updatedConnectionURL = addDelegationTokenUpdateURL(connectionURL, request);
         hiveConnection = DriverManager.getConnection(updatedConnectionURL);

         ps = hiveConnection.prepareStatement(String.format("describe %s.%s", dbName, tableName));
         rs = ps.executeQuery();

         schema = new JsonObject();
         schema.addProperty("type", "record");
         schema.addProperty("name", "etlSchemaBody");

         JsonArray fields = new JsonArray();
         while (rs.next()) {
           JsonObject column = new JsonObject();
           column.addProperty("name", rs.getString("col_name"));
           column.addProperty("type", getCDAPDataType(rs.getString("data_type")));
           fields.add(column);
         }
         schema.add("fields", fields);
       } catch (Exception e) {
         LOG.error("Error fetching hive table schema ", e);
         e.printStackTrace();
       } finally {
         close(rs);
         close(ps);
         close(hiveConnection);
       }
     }

     return schema;
   }

   private String getCDAPDataType(String type){
     if(type.equalsIgnoreCase("long")
             || type.equalsIgnoreCase("bigint")) {
       return Schema.Type.LONG.name();
     }
     if(type.equalsIgnoreCase("int") || type.equalsIgnoreCase("tinyint")
             || type.equalsIgnoreCase("smallint")) {
       return Schema.Type.INT.name();
     }
     return type;
   }

   private String addDelegationTokenUpdateURL(final String jdbcURL, HttpServiceRequest request) throws ClassNotFoundException, LoginException, IOException {
     Class.forName("org.apache.hive.jdbc.HiveDriver");

     StringBuilder updatedJdbcURL = new StringBuilder(jdbcURL);

     UserGroupInformation ugi = Utils.getUGI();
     if(ugi.isSecurityEnabled()){
       if(updatedJdbcURL.indexOf("auth=")==-1) updatedJdbcURL.append(";auth=delegationToken");

       Utils.setTokenStr(ugi,getHiveServer2ClientToken(), HiveAuthFactory.HS2_CLIENT_TOKEN);

       // User impersonation
       if (getContext().getRuntimeArguments().containsKey(ENABLE_USER_IMPERSONATION_CONFIG_KEY)
               && getContext().getRuntimeArguments().get(ENABLE_USER_IMPERSONATION_CONFIG_KEY).equalsIgnoreCase("true")){
         updatedJdbcURL.append(';').append(HiveAuthFactory.HS2_PROXY_USER).append('=').append(request.getHeader(USER_ID));
       }
     }

     if(updatedJdbcURL.indexOf("?")==-1){
       updatedJdbcURL.append('?');
     }else{
       updatedJdbcURL.append('&');
     }
     updatedJdbcURL.append(HIVE_REMOVE_TABLENAME_FROM_COLUMN_NAME_CONFIG_KEY).append("=false");

     LOG.debug("updatedJdbcURL:: " + updatedJdbcURL);

     return updatedJdbcURL.toString();
   }

   private String getHiveServer2ClientToken() throws IOException {
     UserGroupInformation user = UserGroupInformation.getCurrentUser();
     Credentials credentials = user.getCredentials();
     try {
       Token hiveToken = credentials.getToken(new Text(HiveAuthFactory.HS2_CLIENT_TOKEN));
       return  hiveToken.encodeToUrlString();
     } catch (IOException e) {
       LOG.error("Unable to generate delegationToken for hiveserver2ClientToken service", e);
       throw new RuntimeException("Unable to fetch delegation token in HiveUtils");
     }
   }

   private void close(AutoCloseable obj){
     if(obj!=null){
       try { obj.close();
       } catch (Exception e) {
         Log.warn("Error closing", e);
       }
     }
   }

 }
