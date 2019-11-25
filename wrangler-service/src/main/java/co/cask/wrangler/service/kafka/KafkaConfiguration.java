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
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

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

  private String keyDeserializer;
  private String valueDeserializer;
  private String autoOffsetReset;
  private String excludeInternalTopic;
  
  public KafkaConfiguration(Connection conn) {
    keyDeserializer = StringDeserializer.class.getName();
    valueDeserializer = keyDeserializer;
    excludeInternalTopic = "true";
    
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

    String requestTimeoutMs = properties.get(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
    // default the request timeout to 15 seconds, to avoid hanging for minutes
    if (requestTimeoutMs == null) {
      requestTimeoutMs = "15000";
    }
    
    
    Map<String, String> kafkaProducerProperties = new HashMap<>();
    if (properties.containsKey(PropertyIds.KAFAK_PRODUCER_PROPERTIES)) {
    	Type type = new TypeToken<HashMap<String, String>>(){}.getType();
    	kafkaProducerProperties = gson.fromJson(properties.get(PropertyIds.KAFAK_PRODUCER_PROPERTIES), type);
    }
    
    autoOffsetReset = kafkaProducerProperties.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
    if (autoOffsetReset == null) {
    	autoOffsetReset = "earliest";
    }
    
    if (kafkaProducerProperties.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
        keyDeserializer = kafkaProducerProperties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
    }

    if (kafkaProducerProperties.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
        valueDeserializer = kafkaProducerProperties.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
    }
    
    if (kafkaProducerProperties.containsKey(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG)) {
        excludeInternalTopic = kafkaProducerProperties.get(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG);
    }
    
    props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connection);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
    props.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, excludeInternalTopic);
    props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
    
    String keytabLocation = properties.get(PropertyIds.KEYTAB_LOCATION);
    String principal = properties.get(PropertyIds.PRINCIPAL);

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
	}
	
	props.forEach((k,v)->LOG.info("Item : {}, Value :  {}", k, v));
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
