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

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;

import co.cask.cdap.api.data.format.Formats;
import co.cask.wrangler.PropertyIds;
import co.cask.wrangler.dataset.connections.Connection;
import co.cask.wrangler.service.connections.ConnectionType;

/**
 * A class for managing configurations of Kafka.
 */
public final class KafkaConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaConfiguration.class);
  private final String connection;
  private final Properties props;
  private static final Gson gson = new GsonBuilder().create();
  private Map<String, String> kafkaProducerProperties;

  private String keyDeserializer;
  private String valueDeserializer;
  private String autoOffsetReset;
  private String keytabLocation;
  private String principal;
  
  public KafkaConfiguration(Connection conn, Map<String, String> runtimeArgs) {
    
    if (conn.getType() != ConnectionType.KAFKA) {
      throw new IllegalArgumentException(
        String.format("Connection id '%s', name '%s' is not a Kafka configuration.", conn.getId(), conn.getName())
      );
    }
    Map<String, String> properties = conn.getAllProps();
    if(properties == null || properties.size() == 0) {
      throw new IllegalArgumentException("Kafka properties are not defined. Check connection setting.");
    }

    if (properties.containsKey(PropertyIds.BROKER)) {
      connection = properties.get(PropertyIds.BROKER);
    } else {
      throw new IllegalArgumentException("Kafka Brokers not defined.");
    }

    kafkaProducerProperties = new HashMap<>();
    if (properties.containsKey(PropertyIds.KAFAK_PRODUCER_PROPERTIES)) {
    	Type type = new TypeToken<HashMap<String, String>>(){}.getType();
    	kafkaProducerProperties = gson.fromJson(properties.get(PropertyIds.KAFAK_PRODUCER_PROPERTIES), type);
    }
    
    keyDeserializer = StringDeserializer.class.getCanonicalName();
    valueDeserializer = StringDeserializer.class.getCanonicalName();
    
    props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connection);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaProducerProperties.getOrDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
    props.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, kafkaProducerProperties.getOrDefault(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "true"));
    props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, kafkaProducerProperties.getOrDefault(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000"));
    if (!Strings.isNullOrEmpty(properties.get(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name())) && Formats.AVRO.equalsIgnoreCase(properties.get(PropertyIds.FORMAT))) {
    	keyDeserializer = ByteArrayDeserializer.class.getCanonicalName();
    	valueDeserializer = KafkaAvroDeserializer.class.getCanonicalName();
    	props.setProperty(PropertyIds.SCHEMA_NAME, properties.getOrDefault(PropertyIds.SCHEMA_NAME, properties.get(PropertyIds.TOPIC)));
    	props.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), properties.get(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name()));
    	LOG.info("Schema Registry is provided, picking up provided URL as: {}", properties.get(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name()));
    } else if (PropertyIds.BINARY.equalsIgnoreCase(properties.get(PropertyIds.FORMAT)) || Formats.AVRO.equalsIgnoreCase(properties.get(PropertyIds.FORMAT))) {
    	keyDeserializer = ByteArrayDeserializer.class.getCanonicalName();
    	valueDeserializer = ByteArrayDeserializer.class.getCanonicalName();
    }
    
    keyDeserializer = kafkaProducerProperties.getOrDefault(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
    valueDeserializer = kafkaProducerProperties.getOrDefault(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
    
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
    
    keytabLocation = properties.get(PropertyIds.KEYTAB_LOCATION);
    principal = properties.get(PropertyIds.PRINCIPAL);
    if (Strings.isNullOrEmpty(keytabLocation) ^ Strings.isNullOrEmpty(principal)) {
    	throw new IllegalArgumentException("Please specify either both principal and keytab or none.");
    }
    
    if (Strings.isNullOrEmpty(keytabLocation)) {
    	keytabLocation = runtimeArgs.get(PropertyIds.NAMESPACE_KETAB_PATH);
    	principal = runtimeArgs.get(PropertyIds.NAMESPACE_PRINCIPAL_NAME);
        if (Strings.isNullOrEmpty(keytabLocation) ^ Strings.isNullOrEmpty(principal)) {
        	throw new IllegalArgumentException("Please specify either both namespace principal and namespace keytab or none.");
        } else {
        	String[] arr = runtimeArgs.get(PropertyIds.NAMESPACE_KETAB_PATH).split("/");
        	keytabLocation = "./" + arr[arr.length - 1];
        }
    }
    
	//Kerberized environment, need to pass kerberos params
    if(keytabLocation != null && !"".equals(keytabLocation) && principal != null && !"".equals(principal)) {
		
        if (kafkaProducerProperties.containsKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)) {
        	props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, kafkaProducerProperties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        }
		
        if (kafkaProducerProperties.containsKey(SaslConfigs.SASL_KERBEROS_SERVICE_NAME)) {
        	props.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, kafkaProducerProperties.get(SaslConfigs.SASL_KERBEROS_SERVICE_NAME));
        }
		
        if (kafkaProducerProperties.containsKey(SaslConfigs.SASL_MECHANISM)) {
        	props.put(SaslConfigs.SASL_MECHANISM, kafkaProducerProperties.get(SaslConfigs.SASL_MECHANISM));
        }
		
        props.put(SaslConfigs.SASL_JAAS_CONFIG, String.format("com.sun.security.auth.module.Krb5LoginModule required \n" +
				"        useKeyTab=true \n" +
				"        storeKey=true  \n" +
				"        useTicketCache=false  \n" +
				"        renewTicket=true  \n" +
				"        keyTab=\"%s\" \n" +
				"        principal=\"%s\";",
				keytabLocation, principal));
        if (!Strings.isNullOrEmpty(properties.get(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name())) && Formats.AVRO.equalsIgnoreCase(properties.get(PropertyIds.FORMAT))) {
        	props.put(SchemaRegistryClient.Configuration.SASL_JAAS_CONFIG.name(), String.format("com.sun.security.auth.module.Krb5LoginModule required \n" +
        			"        useKeyTab=true \n" +
        			"        storeKey=true  \n" +
        			"        useTicketCache=false  \n" +
        			"        keyTab=\"%s\" \n" +
        			"        principal=\"%s\";",
        			keytabLocation, principal));
        }
	}
	
	props.forEach((k,v)->LOG.debug("Item : {}, Value :  {}", k, v));
  }
  
  
  /**
   * @return keytab location
   */
  public String getKeytabLocation() {
	return keytabLocation;
  }

  /**
   * @return Principal
   */
  public String getPrincipal() {
	return principal;
  }

  /**
   * @return String representation of key deserializer of kafka topic.
   */
  public String getKeyDeserializer() {
    return keyDeserializer;
  }

  /**
   * @return String representation of value deserializer of kafka topic.
   */
  public String getValueDeserializer() {
    return valueDeserializer;
  }
  
  
  /**
   * @return Map of extra kafka properties passed from UI
   */
  public Map<String, String> getKafkaProducerProperties() {
	return kafkaProducerProperties;
  }

  private String deserialize(String type) {
    type = type.toLowerCase();
    switch(type) {
      case "string":
        return StringDeserializer.class.getName();

      case "int":
        return IntegerDeserializer.class.getName();

      case "long":
        return LongDeserializer.class.getName();

      case "double":
        return DoubleDeserializer.class.getName();

      case "bytes":
        return ByteArrayDeserializer.class.getName();

      default:
        throw new IllegalArgumentException(
          String.format("Deserializer '%s' type not supported.", type)
        );
    }
  }

  /**
   * @return connection information of kafka.
   */
  public String getConnection() {
    return connection;
  }

  /**
   * @return Kafka connection property.
   */
  public Properties get() {
    return props;
  }
}
