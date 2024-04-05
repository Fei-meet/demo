package com.example.acp_submission_2;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.errors.TimeoutException;

import java.util.*;
import java.util.concurrent.ExecutionException;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@RestController
public class MyController {
    private static final Logger log = LoggerFactory.getLogger(MyController.class);

    @PostMapping("/readTopic/{topicName}")
    public ResponseEntity<String> readTopicFromPost(@PathVariable String topicName, @RequestBody KafkaPropertiesDTO kafkaProperties) {
        if (kafkaProperties.getBootstrapServers() == null ||
                kafkaProperties.getSecurityProtocol() == null ||
                kafkaProperties.getSaslMechanism() == null || kafkaProperties.getGroupId() ==null) {
            return ResponseEntity.badRequest().body("Missing required fields in request body. We need:\n- bootstrap.servers\n- sasl.jaas.config\n- security.protocol \n- sasl.mechanism\n- group.id");
        }
        if (!Objects.equals(kafkaProperties.getSecurityProtocol(), "SASL_SSL")){
            return ResponseEntity.badRequest().body("Wrong security.protocol! It should be: 'SASL_SSL'!");
        }
        if (!Objects.equals(kafkaProperties.getSaslMechanism(), "PLAIN")){
            return ResponseEntity.badRequest().body("Wrong sasl.mechanism! It should be: 'PLAIN'!");
        }
        try {
            Properties props = new Properties();
            props.put("bootstrap.servers", kafkaProperties.getBootstrapServers());
            props.put("security.protocol", kafkaProperties.getSecurityProtocol());
            props.put("sasl.jaas.config", kafkaProperties.getSaslJaasConfig());
            props.put("sasl.mechanism", kafkaProperties.getSaslMechanism());
            props.put("group.id", kafkaProperties.getGroupId());
            props.put("auto.offset.reset", "latest");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            try (AdminClient adminClient = AdminClient.create(props)) {
                ListTopicsResult listTopics = adminClient.listTopics();
                Set<String> topics = listTopics.names().get(); // 这个操作是阻塞的

                if (!topics.contains(topicName)) {
                    log.info("Topic does not exist: " + topicName);
                    return ResponseEntity.badRequest().body("Topic does not exist: " + topicName);
                }
            } catch (InterruptedException | ExecutionException e) {
                if (e.getCause() instanceof org.apache.kafka.common.errors.SaslAuthenticationException) {
                    return ResponseEntity.badRequest().body("Authentication failed. Please check your credentials.");
                }
                Thread.currentThread().interrupt();
                log.error("Failed to check if topic exists", e);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to check if topic exists: " + e.getMessage());
            }
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(topicName));

            StringBuilder builder = new StringBuilder();
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

            for (ConsumerRecord<String, String> record : records) {
                builder.append(String.format("[%s] Key: %s, Value: %s, partition: %s,offset: %s,timestamp: %s%n",
                        record.topic(), record.key(), record.value(),
                        record.partition(), record.offset(), record.timestamp()));
            }
            consumer.close();
            return ResponseEntity.ok(builder.toString());
        } catch (Exception e) {
            log.error("Error while reading from Kafka topic", e);
            return ResponseEntity.badRequest().body("Invalid request, please check: " + e.getMessage());
        }
    }

    @PostMapping("/writeTopic/{topicName}/{data}")
    public ResponseEntity<String> writeTopicFromPost(@PathVariable String topicName, @PathVariable String data,@RequestBody KafkaPropertiesDTO kafkaProperties) {
        if (kafkaProperties.getBootstrapServers() == null ||
                kafkaProperties.getSecurityProtocol() == null ||
                kafkaProperties.getSaslMechanism() == null) {
            return ResponseEntity.badRequest().body("Missing required fields in request body. We need:\n- bootstrap.servers\n- sasl.jaas.config\n- security.protocol \n- sasl.mechanism");
        }
        if (!Objects.equals(kafkaProperties.getSecurityProtocol(), "SASL_SSL")){
            return ResponseEntity.badRequest().body("Wrong security.protocol! It should be: 'SASL_SSL'!");
        }
        if (!Objects.equals(kafkaProperties.getSaslMechanism(), "PLAIN")){
            return ResponseEntity.badRequest().body("Wrong sasl.mechanism! It should be: 'PLAIN'!");
        }
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaProperties.getBootstrapServers());
        props.put("security.protocol", kafkaProperties.getSecurityProtocol());
        props.put("sasl.jaas.config", kafkaProperties.getSaslJaasConfig());
        props.put("sasl.mechanism", kafkaProperties.getSaslMechanism());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (AdminClient adminClient = AdminClient.create(props)) {
            ListTopicsResult listTopics = adminClient.listTopics();
            Set<String> topics = listTopics.names().get();

            if (!topics.contains(topicName)) {
                log.info("Topic does not exist: " + topicName);
                return ResponseEntity.badRequest().body("Topic does not exist: " + topicName);
            }
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.SaslAuthenticationException) {
                return ResponseEntity.badRequest().body("Authentication failed. Please check your credentials.");
            }
            Thread.currentThread().interrupt();
            log.error("Failed to check if topic exists", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to check if topic exists: " + e.getMessage());
        }catch (Exception e) {
            log.error("Error while reading from Kafka topic", e);
            return ResponseEntity.badRequest().body("Invalid request, please check: " + e.getMessage());
        }

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName,"s2511180",data);
            // 同步发送消息，并等待响应
            RecordMetadata metadata = producer.send(record).get();
            producer.close();
            return ResponseEntity.ok(String.format("Message sent to topic %s: partition: %s with offset: %s",
                    metadata.topic(), metadata.partition(), metadata.offset()));
        } catch (Exception e) {
            log.error("Error while reading from Kafka topic", e);
            return ResponseEntity.badRequest().body("Invalid request, please check: " + e.getMessage());
        }
    }

    @PostMapping("transformMessage/{readTopic}/{writeTopic}")
    public ResponseEntity<String> transformMessageFromPost(@PathVariable String readTopic, @PathVariable String writeTopic,@RequestBody KafkaPropertiesDTO kafkaProperties) {
        if (kafkaProperties.getBootstrapServers() == null ||
                kafkaProperties.getSecurityProtocol() == null ||
                kafkaProperties.getSaslMechanism() == null || kafkaProperties.getGroupId() ==null) {
            return ResponseEntity.badRequest().body("Missing required fields in request body. We need:\n- bootstrap.servers\n- sasl.jaas.config\n- security.protocol \n- sasl.mechanism\n- group.id");
        }
        if (!Objects.equals(kafkaProperties.getSecurityProtocol(), "SASL_SSL")){
            return ResponseEntity.badRequest().body("Wrong security.protocol! It should be: 'SASL_SSL'!");
        }
        if (!Objects.equals(kafkaProperties.getSaslMechanism(), "PLAIN")){
            return ResponseEntity.badRequest().body("Wrong sasl.mechanism! It should be: 'PLAIN'!");
        }
        Properties read_props = new Properties();
        read_props.put("bootstrap.servers", kafkaProperties.getBootstrapServers());
        read_props.put("security.protocol", kafkaProperties.getSecurityProtocol());
        read_props.put("sasl.jaas.config", kafkaProperties.getSaslJaasConfig());
        read_props.put("sasl.mechanism", kafkaProperties.getSaslMechanism());
        read_props.put("group.id", kafkaProperties.getGroupId());
        read_props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        read_props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Properties write_props = new Properties();
        write_props.put("bootstrap.servers", kafkaProperties.getBootstrapServers());
        write_props.put("security.protocol", kafkaProperties.getSecurityProtocol());
        write_props.put("sasl.jaas.config", kafkaProperties.getSaslJaasConfig());
        write_props.put("sasl.mechanism", kafkaProperties.getSaslMechanism());
        write_props.put("group.id", kafkaProperties.getGroupId());
        write_props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        write_props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        StringBuilder responseBuilder = new StringBuilder();

        try (AdminClient adminClient = AdminClient.create(read_props)) {
            ListTopicsResult listTopics = adminClient.listTopics();
            Set<String> topics = listTopics.names().get();

            if (!topics.contains(readTopic) ||!topics.contains(writeTopic)) {
                log.info("Topic does not exist " );
                return ResponseEntity.badRequest().body("Topic does not exist ");
            }
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.SaslAuthenticationException) {
                return ResponseEntity.badRequest().body("Authentication failed. Please check your credentials.");
            }
            Thread.currentThread().interrupt();
            log.error("Failed to check if topic exists", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to check if topic exists: " + e.getMessage());
        }catch (Exception e) {
            log.error("Error while reading from Kafka topic", e);
            return ResponseEntity.badRequest().body("Invalid request, please check: " + e.getMessage());
        }
        // 创建消费者
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(read_props)) {
            consumer.subscribe(Collections.singletonList(readTopic));

            // 创建生产者
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(write_props)) {
                // 从readTopic读取数据
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
                if (records.isEmpty()) {
                    return ResponseEntity.ok("No messages to transform from " + readTopic);
                }
                records.forEach(record -> {
                    String originalData = record.value();
                    String transformedData = originalData.toUpperCase();
                    producer.send(new ProducerRecord<>(writeTopic, transformedData));
                    responseBuilder.append(String.format("Value %s from readTopic: %s has transformed into %s to writeTopic: %s.%n",
                            originalData, readTopic, transformedData, writeTopic));
                });

                return ResponseEntity.ok(responseBuilder.toString());
            }
        } catch (TimeoutException e) {
            log.error("Timeout while attempting to communicate with Kafka cluster", e);
            return ResponseEntity.badRequest().body("Timeout while attempting to communicate with Kafka cluster: " + e.getMessage());
        } catch (Exception e) {
            log.error("Error while reading from Kafka topic", e);
            return ResponseEntity.badRequest().body("Invalid request, please check: " + e.getMessage());
        }
    }

    @PostMapping("store/{readTopic}/{writeTopic}")
    public ResponseEntity<String> storeFromPost(@PathVariable String readTopic, @PathVariable String writeTopic,@RequestBody KafkaPropertiesDTO kafkaProperties) {
        if (kafkaProperties.getBootstrapServers() == null ||
                kafkaProperties.getSecurityProtocol() == null ||
                kafkaProperties.getSaslMechanism() == null || kafkaProperties.getGroupId() == null) {
            return ResponseEntity.badRequest().body("Missing required fields in request body. We need:\n- bootstrap.servers\n- sasl.jaas.config\n- security.protocol \n- sasl.mechanism\n- group.id (StorageServer has initial value)");
        }
        if (!Objects.equals(kafkaProperties.getSecurityProtocol(), "SASL_SSL")){
            return ResponseEntity.badRequest().body("Wrong security.protocol! It should be: 'SASL_SSL'!");
        }
        if (!Objects.equals(kafkaProperties.getSaslMechanism(), "PLAIN")){
            return ResponseEntity.badRequest().body("Wrong sasl.mechanism! It should be: 'PLAIN'!");
        }
        Properties read_props = new Properties();
        read_props.put("bootstrap.servers", kafkaProperties.getBootstrapServers());
        read_props.put("security.protocol", kafkaProperties.getSecurityProtocol());
        read_props.put("sasl.jaas.config", kafkaProperties.getSaslJaasConfig());
        read_props.put("sasl.mechanism", kafkaProperties.getSaslMechanism());
        read_props.put("group.id", kafkaProperties.getGroupId());
        read_props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        read_props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Properties write_props = new Properties();
        write_props.put("bootstrap.servers", kafkaProperties.getBootstrapServers());
        write_props.put("security.protocol", kafkaProperties.getSecurityProtocol());
        write_props.put("sasl.jaas.config", kafkaProperties.getSaslJaasConfig());
        write_props.put("sasl.mechanism", kafkaProperties.getSaslMechanism());
        write_props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        write_props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        StringBuilder responseBuilder = new StringBuilder();

        try (AdminClient adminClient = AdminClient.create(read_props)) {
            ListTopicsResult listTopics = adminClient.listTopics();
            Set<String> topics = listTopics.names().get();

            if (!topics.contains(readTopic) ||!topics.contains(writeTopic)) {
                log.info("Topic does not exist " );
                return ResponseEntity.badRequest().body("Topic does not exist ");
            }
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.SaslAuthenticationException) {
                return ResponseEntity.badRequest().body("Authentication failed. Please check your credentials.");
            }
            Thread.currentThread().interrupt();
            log.error("Failed to check if topic exists", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to check if topic exists: " + e.getMessage());
        }catch (Exception e) {
            log.error("Error while reading from Kafka topic", e);
            return ResponseEntity.badRequest().body("Invalid request, please check: " + e.getMessage());
        }

        // 创建消费者
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(read_props)) {
            consumer.subscribe(Collections.singletonList(readTopic));

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(write_props)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
                if (records.isEmpty()) {
                    return ResponseEntity.ok("No messages to transform from " + readTopic);
                }

                Gson gson = new Gson();
                records.forEach(record -> {
                    String dataFromTopic = record.value();

                    // 构建JSON请求体
                    Map<String, String> requestBodyMap = new HashMap<>();
                    requestBodyMap.put("uid", "s2511180");
                    requestBodyMap.put("datasetName", "ACP_CW2");
                    requestBodyMap.put("data", dataFromTopic);
                    String jsonRequestBody = gson.toJson(requestBodyMap);

                    String baseUrl = kafkaProperties.getStorageServer(); // 确保KafkaPropertiesDTO有getBaseUrl()方法
                    String requestUrl = baseUrl + "/write/blob";

                    // 发送POST请求并处理响应
                    try {
                        URL url = new URL(requestUrl);
                        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                        conn.setRequestMethod("POST");
                        conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
                        conn.setDoOutput(true);

                        try(OutputStream os = conn.getOutputStream()) {
                            byte[] input = jsonRequestBody.getBytes("utf-8");
                            os.write(input, 0, input.length);
                        }

                        // 读取响应
                        try(BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), "utf-8"))) {
                            StringBuilder response = new StringBuilder();
                            String responseLine;
                            while ((responseLine = br.readLine()) != null) {
                                response.append(responseLine.trim());
                            }
                            String responseUUID = response.toString();

                            // 将响应UUID写入writeTopic
                            producer.send(new ProducerRecord<>(writeTopic, responseUUID));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

                return ResponseEntity.ok("Data processed and stored.");
            }
        } catch (Exception e) {
            log.error("Error while reading from Kafka topic", e);
            return ResponseEntity.badRequest().body("Invalid request, please check: " + e.getMessage());
        }

    }

    @PostMapping("retrieve/{writeTopic}/{uuid}")
    public ResponseEntity<String> retrieveFromPost(@PathVariable String writeTopic,@PathVariable String uuid,@RequestBody KafkaPropertiesDTO kafkaProperties) {
        if (kafkaProperties.getBootstrapServers() == null ||
                kafkaProperties.getSecurityProtocol() == null ||
                kafkaProperties.getSaslMechanism() == null) {
            return ResponseEntity.badRequest().body("Missing required fields in request body. We need:\n- bootstrap.servers\n- sasl.jaas.config\n- security.protocol \n- sasl.mechanism");
        }
        if (!Objects.equals(kafkaProperties.getSecurityProtocol(), "SASL_SSL")){
            return ResponseEntity.badRequest().body("Wrong security.protocol! It should be: 'SASL_SSL'!");
        }
        if (!Objects.equals(kafkaProperties.getSaslMechanism(), "PLAIN")){
            return ResponseEntity.badRequest().body("Wrong sasl.mechanism! It should be: 'PLAIN'!");
        }
        Properties write_props = new Properties();
        write_props.put("bootstrap.servers", kafkaProperties.getBootstrapServers());
        write_props.put("security.protocol", kafkaProperties.getSecurityProtocol());
        write_props.put("sasl.jaas.config", kafkaProperties.getSaslJaasConfig());
        write_props.put("sasl.mechanism", kafkaProperties.getSaslMechanism());
        write_props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        write_props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (AdminClient adminClient = AdminClient.create(write_props)) {
            ListTopicsResult listTopics = adminClient.listTopics();
            Set<String> topics = listTopics.names().get(); // 这个操作是阻塞的

            if (!topics.contains(writeTopic)) {
                log.info("Topic does not exist: " + writeTopic);
                return ResponseEntity.badRequest().body("Topic does not exist: " + writeTopic);
            }
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.SaslAuthenticationException) {
                return ResponseEntity.badRequest().body("Authentication failed. Please check your credentials.");
            }
            Thread.currentThread().interrupt();
            log.error("Failed to check if topic exists", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to check if topic exists: " + e.getMessage());
        }catch (Exception e) {
            log.error("Error while reading from Kafka topic", e);
            return ResponseEntity.badRequest().body("Invalid request, please check: " + e.getMessage());
        }

        try {
            // 构建存储服务的URL，这里你可以根据实际情况调整baseUrl
            String baseUrl = kafkaProperties.getStorageServer();
            String fullUrl = baseUrl + "/read/blob/" + uuid;

            // 初始化HTTP连接并发送GET请求
            URL url = new URL(fullUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            // 读取响应内容
            StringBuilder responseBuilder = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    responseBuilder.append(line);
                }
            }

            // 获取BLOB数据
            String blobData = responseBuilder.toString();

            // 使用Kafka生产者发送BLOB数据到指定的主题
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(write_props)) {
                producer.send(new ProducerRecord<>(writeTopic, "s2511180", blobData));
            }

            // 返回操作结果
            return ResponseEntity.ok("BLOB data successfully retrieved and sent to Kafka topic: " + writeTopic);
        } catch (Exception e) {
            log.error("Error while reading from Kafka topic", e);
            return ResponseEntity.badRequest().body("Invalid request, please check: " + e.getMessage());
        }
    }


    public static class KafkaPropertiesDTO {
        @JsonProperty("bootstrap.servers")
        private String bootstrapServers;

        @JsonProperty("sasl.jaas.config")
        private String saslJaasConfig;

        @JsonProperty("security.protocol")
        private String securityProtocol;

        @JsonProperty("sasl.mechanism")
        private String saslMechanism;

        @JsonProperty("group.id")
        private String groupId;

        @JsonProperty("storage.server")
        private String storageServer;

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getSecurityProtocol() {
            return securityProtocol;
        }

        public void setSecurityProtocol(String securityProtocol) {
            this.securityProtocol = securityProtocol;
        }

        public String getSaslJaasConfig() {
            return saslJaasConfig;
        }

        public void setSaslJaasConfig(String saslJaasConfig) {
            this.saslJaasConfig = saslJaasConfig;
        }

        public String getSaslMechanism() {
            return saslMechanism;
        }

        public void setSaslMechanism(String saslMechanism) {
            this.saslMechanism = saslMechanism;
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public String getStorageServer() {
            if (this.storageServer == null || this.storageServer.isEmpty()) {
                return "https://acp-storage.azurewebsites.net";
            }
            return storageServer;
        }

        public void setStorageServer(String storageServer) {
            this.storageServer = storageServer;
        }
    }

}
