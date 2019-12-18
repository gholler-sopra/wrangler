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

package co.cask.wrangler.service.kafka;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.wrangler.DataPrep;
import co.cask.wrangler.PropertyIds;
import co.cask.wrangler.RequestExtractor;
import co.cask.wrangler.SamplingMethod;
import co.cask.wrangler.ServiceUtils;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.dataset.connections.Connection;
import co.cask.wrangler.dataset.connections.ConnectionStore;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.service.common.Format;
import co.cask.wrangler.service.connections.ConnectionType;

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;
import co.cask.wrangler.utils.ObjectSerDe;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.reflect.TypeToken;
import com.guavus.utils.KerberosHttpClient;
import com.guavus.utils.URLExtractor;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.yarn.util.Records;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import static co.cask.wrangler.ServiceUtils.error;
import static co.cask.wrangler.ServiceUtils.sendJson;
import static co.cask.wrangler.service.directive.DirectivesService.WORKSPACE_DATASET;

/**
 * Service for handling Kafka connections.
 */
public final class KafkaService extends AbstractHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaService.class);
  private static final Gson gson =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
  private Map<String, String> runtimeArgs;
  private Schema outputSchema;

  @UseDataSet(WORKSPACE_DATASET)
  private WorkspaceDataset ws;

  // Data Prep store which stores all the information associated with dataprep.
  @UseDataSet(DataPrep.CONNECTIONS_DATASET)
  private Table table;

  // Abstraction over the table defined above for managing connections.
  private ConnectionStore store;

  /**
   * Stores the context so that it can be used later.
   *
   * @param context the HTTP service runtime context
   */
  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    runtimeArgs = context.getRuntimeArguments();
    store = new ConnectionStore(table);
  }

  /**
   * Tests the connection to kafka..
   *
   * Following is the response when the connection is successful.
   *
   * {
   *   "status" : 200,
   *   "message" : "Successfully connected to kafka."
   * }
   *
   * @param request  HTTP request handler.
   * @param responder HTTP response handler.
   */
  @POST
  @Path("connections/kafka/test")
  public void test(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      Connection connection = extractor.getContent("utf-8", Connection.class);

      if (ConnectionType.fromString(connection.getType().getType()) == ConnectionType.UNDEFINED) {
        error(responder, "Invalid connection type set.");
        return;
      }

      KafkaConfiguration config = new KafkaConfiguration(connection, runtimeArgs);
      Properties props = config.get();

      // Checks connection by extracting topics.
      try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
        consumer.listTopics();
      }
      ServiceUtils.success(responder, String.format("Successfully connected to Kafka at %s", config.getConnection()));
    } catch (Exception e) {
      ServiceUtils.error(responder, e.getMessage());
    }
  }

  /**
   * List all kafka topics.
   *
   * @param request HTTP requets handler.
   * @param responder HTTP response handler.
   */
  @POST
  @Path("connections/kafka")
  public void list(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      Connection connection = extractor.getContent("utf-8", Connection.class);

      if (ConnectionType.fromString(connection.getType().getType()) == ConnectionType.UNDEFINED) {
        error(responder, "Invalid connection type set.");
        return;
      }

      KafkaConfiguration config = new KafkaConfiguration(connection, runtimeArgs);
      Properties props = config.get();

      // Extract topics from Kafka.
      try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
        Map<String, List<PartitionInfo>> topics = consumer.listTopics();

        // Prepare response.
        JsonArray values = new JsonArray();
        for (String topic : topics.keySet()) {
          values.add(new JsonPrimitive(topic));
        }

        JsonObject response = new JsonObject();
        response.addProperty(PropertyIds.STATUS, HttpURLConnection.HTTP_OK);
        response.addProperty(PropertyIds.MESSAGE, PropertyIds.SUCCESS);
        response.addProperty(PropertyIds.COUNT, values.size());
        response.add(PropertyIds.VALUES, values);
        ServiceUtils.sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
      }
    } catch (Exception e) {
      LOG.error("Error while fetching topic list", e);
      ServiceUtils.error(responder, e.getMessage());
    }
  }

  /**
   * Reads a kafka topic into workspace.
   *
   * @param request HTTP requets handler.
   * @param responder HTTP response handler.
   * @param id Connection id for which the tables need to be listed from database.
   */
  @GET
  @Path("connections/{id}/kafka/{topic}/read")
  public void read(HttpServiceRequest request, HttpServiceResponder responder,
                   @PathParam("id") String id, @PathParam("topic") String topic,
                   @QueryParam("lines") int lines,
                   @QueryParam("scope") String scope) {
      try {
        Connection connection = store.get(id);
        if (connection == null) {
          error(responder, String.format("Invalid connection id '%s' specified or connection does not exist.", id));
          return;
        }

        if (scope == null || scope.isEmpty()) {
          scope = WorkspaceDataset.DEFAULT_SCOPE;
        }

        String uuid = ServiceUtils.generateMD5(String.format("%s:%s.%s", scope, id, topic));
        ws.createWorkspaceMeta(uuid, scope, topic);
        KafkaConfiguration config = new KafkaConfiguration(connection, runtimeArgs);
        List<Row> recs = new ArrayList<>();
        boolean running = true;
        outputSchema = generateSchema(config, connection.getAllProps().get(PropertyIds.FORMAT));
        if (!Strings.isNullOrEmpty(connection.getAllProps().get(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name())) && Formats.AVRO.equalsIgnoreCase(connection.getAllProps().get(PropertyIds.FORMAT))) {
          LOG.debug("Reading avro data from topic: {} with schema registry url: {}", topic, connection.getAllProps().get(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name()));
          KafkaConsumer<byte[], Object> consumer = new KafkaConsumer<>(config.get());
          FetchData<byte[], Object> fetchData = new FetchData<>(consumer);
          recs = fetchData.fetchData(topic, lines);
        } else if ("avro".equalsIgnoreCase(connection.getAllProps().get(PropertyIds.FORMAT)) || PropertyIds.BINARY.equalsIgnoreCase(connection.getAllProps().get(PropertyIds.FORMAT))) {
          LOG.debug("Reading avro/binary data from topic: {}", topic);
          KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config.get());
          FetchData<byte[], byte[]> fetchData = new FetchData<>(consumer);
          recs = fetchData.fetchData(topic, lines);
        } else {
          LOG.debug("Reading text/json/csv data from topic: {}", topic);
          KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config.get());
          FetchData<String, String> fetchData = new FetchData<>(consumer);
          recs = fetchData.fetchData(topic, lines);
        }

        ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
        byte[] data = serDe.toByteArray(recs);
        ws.writeToWorkspace(uuid, WorkspaceDataset.DATA_COL, DataType.RECORDS, data);

        // Set all properties and write to workspace.
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyIds.ID, uuid);
        properties.put(PropertyIds.NAME, topic);
        properties.put(PropertyIds.CONNECTION_ID, id);
        properties.put(PropertyIds.TOPIC, topic);
        properties.put(PropertyIds.BROKER, config.getConnection());
        properties.put(PropertyIds.CONNECTION_TYPE, connection.getType().getType());
        properties.put(PropertyIds.KEY_DESERIALIZER, config.getKeyDeserializer());
        properties.put(PropertyIds.VALUE_DESERIALIZER, config.getValueDeserializer());
        properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.FIRST.getMethod());
        ws.writeProperties(uuid, properties);

        JsonObject response = new JsonObject();
        JsonArray values = new JsonArray();
        JsonObject object = new JsonObject();
        object.addProperty(PropertyIds.ID, uuid);
        object.addProperty(PropertyIds.NAME, topic);
        values.add(object);
        response.addProperty(PropertyIds.STATUS, HttpURLConnection.HTTP_OK);
        response.addProperty(PropertyIds.MESSAGE, PropertyIds.SUCCESS);
        response.addProperty(PropertyIds.COUNT, values.size());
        response.add(PropertyIds.VALUES, values);
        sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (Throwable e) {
      LOG.error("Exception while reading data from kafka topic", e);
      error(responder, e.getMessage());
    }
  }
  
  
  
  public Schema generateSchema(KafkaConfiguration config, String format) throws Exception {
	Properties properties = config.get();
	if (!format.equalsIgnoreCase(Formats.AVRO) && !format.equalsIgnoreCase(PropertyIds.BINARY)) {
		return Format.TEXT.getSchema();
	} else if ((format.equalsIgnoreCase(Formats.AVRO) && !properties.containsKey(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name())) || format.equalsIgnoreCase(PropertyIds.BINARY)){
		return Format.BLOB.getSchema();
	}
    String schemaName = properties.getProperty(PropertyIds.SCHEMA_NAME);
    if (schemaName == null || schemaName.trim().isEmpty()) {
      throw new IllegalArgumentException("Empty schema name");
    }

    String schemaRegistryUrl = properties.getProperty(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name());
    LOG.error("Schema Registry Url: {}", schemaRegistryUrl);
    if (schemaRegistryUrl == null || schemaRegistryUrl.trim().isEmpty()) {
      throw new IllegalArgumentException("Empty schema registry url");
    }

    schemaRegistryUrl = schemaRegistryUrl.trim();
    if (!schemaRegistryUrl.endsWith("/")) {
      schemaRegistryUrl = schemaRegistryUrl + "/";
    }

    schemaRegistryUrl = schemaRegistryUrl + "schemaregistry/schemas/" + schemaName + "/versions/latest";

    String schemaString = "";
    String keytabLocation = config.getKeytabLocation();
    String principal = config.getPrincipal();
    if(Strings.isNullOrEmpty(keytabLocation) || Strings.isNullOrEmpty(principal)) {
       schemaString = URLExtractor.extract(schemaRegistryUrl, 30);
    } else {
       schemaString = KerberosHttpClient.sendKerberisedGetRequest(schemaRegistryUrl,
    		   principal, keytabLocation);
    }
    return parseSchemaString(schemaString);
  }

  private Schema parseSchemaString(String schemaString) throws IOException {
    Config config = ConfigFactory.parseString(schemaString);
    if (config.hasPath("schemaText")) {
      try {
        List<Schema.Field> schemaFields = Schema.parseJson(config.getString("schemaText")).getFields();
        return Schema.recordOf("etlSchemaBody", schemaFields);
      } catch (IOException e) {
    	LOG.error(e.getMessage(), e);
    	throw e;
	  }
	} else {
	  throw new IllegalStateException("'schemaText' is not present in schema json.");
	}
  }
  
  /**
   * Specification for the source.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param id of the connection.
   * @param topic for which the specification need to be generated.
   */
  @Path("connections/{id}/kafka/{topic}/specification")
  @GET
  public void specification(HttpServiceRequest request, final HttpServiceResponder responder,
                            @PathParam("id") String id, @PathParam("topic") final String topic) {
    JsonObject response = new JsonObject();
    String format = Formats.TEXT;
    
    try {
      Connection conn = store.get(id);
      JsonObject value = new JsonObject();
      JsonObject kafka = new JsonObject();

      Map<String, String> properties = new HashMap<>();
      properties.put(PropertyIds.TOPIC, topic);
      properties.put("referenceName", topic);
      properties.put("brokers", conn.getProp(PropertyIds.BROKER));
      properties.put("kafkaBrokers", conn.getProp(PropertyIds.BROKER));
      properties.put("keyField", conn.getProp(PropertyIds.KEY_DESERIALIZER));
      properties.put("defaultInitialOffset", "-2");
      String keytabLocation = conn.getAllProps().getOrDefault(PropertyIds.KEYTAB_LOCATION, runtimeArgs.get(PropertyIds.NAMESPACE_KETAB_PATH));
      String principal = conn.getAllProps().getOrDefault(PropertyIds.PRINCIPAL, runtimeArgs.get(PropertyIds.NAMESPACE_PRINCIPAL_NAME));
      if (keytabLocation != null && principal != null) {
    	  properties.put(PropertyIds.KEYTAB_LOCATION, keytabLocation);
    	  properties.put(PropertyIds.PRINCIPAL, principal);
      }

      if (conn.getAllProps().containsKey(PropertyIds.KAFAK_PRODUCER_PROPERTIES)) {
    	  Type type = new TypeToken<HashMap<String, String>>(){}.getType();
    	  Map<String, String> kafkaProducerProperties = gson.fromJson(conn.getAllProps().get(PropertyIds.KAFAK_PRODUCER_PROPERTIES), type);
    	  properties.put(PropertyIds.KAFKA_PROPERTIES, Joiner.on(",").withKeyValueSeparator(":").join(kafkaProducerProperties));
      }
      outputSchema = generateSchema(new KafkaConfiguration(conn, runtimeArgs), conn.getAllProps().get(PropertyIds.FORMAT));
      
      if (format.equalsIgnoreCase(Formats.AVRO) && !Strings.isNullOrEmpty(conn.getAllProps().get(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name()))) {
    	  properties.put("schemaRegistryUrl", conn.getAllProps().get(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name()));
    	  properties.put("schemaName", conn.getAllProps().getOrDefault(PropertyIds.SCHEMA_NAME, topic));
      }
      
      if (conn.getAllProps().containsKey(PropertyIds.FORMAT) && Arrays.asList(PropertyIds.BINARY, Formats.AVRO).contains(conn.getAllProps().get(PropertyIds.FORMAT).toLowerCase())) {
          format = conn.getAllProps().get(PropertyIds.FORMAT).toLowerCase();
      }
      
      properties.put("schema", outputSchema.toString());
      properties.put(PropertyIds.FORMAT, format);
      kafka.addProperty(PropertyIds.NAME, "Kafka");
      kafka.addProperty("type", "source");
      kafka.add("properties", gson.toJsonTree(properties));
      value.add("Kafka", kafka);

      JsonArray values = new JsonArray();
      values.add(value);
      response.addProperty(PropertyIds.STATUS, HttpURLConnection.HTTP_OK);
      response.addProperty(PropertyIds.MESSAGE, PropertyIds.SUCCESS);
      response.addProperty(PropertyIds.COUNT, values.size());
      response.add(PropertyIds.VALUES, values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (Throwable e) {
      LOG.error("Exception while setting plugin specification", e);
      error(responder, e.getMessage());
    }
  }
  
  class FetchData<K, V> {

    KafkaConsumer<K, V> consumer;

	FetchData(KafkaConsumer<K, V> consumer) {
      this.consumer = consumer;
    }

	public List<Row> fetchData(String topic, Integer count) throws Exception {
      List<Row> recs = new ArrayList<>();
      consumer.subscribe(Lists.newArrayList(topic));
      try {
        ConsumerRecords<K, V> records = consumer.poll(10000);
        LOG.debug("Record count received is: {}", records.count());
        List<String> outputFields = new ArrayList<>();
        for (co.cask.cdap.api.data.schema.Schema.Field field: outputSchema.getFields()) {
          outputFields.add(field.getName());
        }
        for (ConsumerRecord<K, V> record : records) {
          Row rec = new Row();
          V value = record.value();
          if (value instanceof GenericRecord) {
            parseAvroRecord(rec, value, outputFields);
          } else {
            rec.add(outputFields.get(0), value);
          }

          recs.add(rec);
          if (count < 0) {
            break;
          }
          count--;
        }
      } catch (Exception e) {
        LOG.error("Exception while reading data from topic {}, {}", topic, e.getMessage());
        throw e;
      } finally {
        consumer.close();
      }
      return recs;
	}

    private void parseAvroRecord(Row rec, V value, List<String> outputFields) {
      GenericRecord messageRecord = (GenericRecord) value;
      List<Field> fields = messageRecord.getSchema().getFields();
      for (Field field : fields) {
        String fieldName = field.name();

        if (outputFields.contains(fieldName)) {
          Object fieldValue = messageRecord.get(fieldName);
          // In case of String values it was observed that sometimes
          // value from kafka
          // comes as type Utf8 which is not a serializable class.
          // Convert this to string in the
          // builder
          if (fieldValue instanceof org.apache.avro.util.Utf8) {
            fieldValue = ((org.apache.avro.util.Utf8) fieldValue).toString();
          } else if (fieldValue instanceof GenericData.Array) {
        	parseArray(fieldValue);
          }
          rec.add(fieldName, fieldValue);
        }
      }
    }

    private void parseArray(Object fieldValue) {
      GenericData.Array arrData = (GenericData.Array) fieldValue;
      if (!arrData.isEmpty() && arrData.get(0) instanceof org.apache.avro.util.Utf8) {
        for (int i = 0; i < arrData.size(); i++) {
          String newArrayType = ((org.apache.avro.util.Utf8) arrData.get(i)).toString();
          arrData.set(i, newArrayType);
        }
      }
    }
  }
}
