package com.example.demo;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.web.bind.annotation.*;

import java.util.Properties;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;


@RestController
public class MyController {

    @GetMapping("/uuid")
    public ResponseEntity<String> getStudentId_by_uuid() {
        return ResponseEntity.ok("<h1>s2511180</h1>");
    }

    @GetMapping("/writeTopic/{topicName}/{data}")
    public ResponseEntity<String> writeTopic(@PathVariable String topicName, @PathVariable String data) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-l6wr6.europe-west2.gcp.confluent.cloud:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='5GECTOGY2ARAL2WS' password='Az7Sjgz57SHqfGYwBA9vE1yfPRTt1iF51OvQcngAn9vok4vw62zF6WarZPQz78Wr';");
        props.put("sasl.mechanism", "PLAIN");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, data);
            // 同步发送消息，并等待响应
            RecordMetadata metadata = producer.send(record).get();
            return ResponseEntity.ok(String.format("Message sent to topic %s partition %s with offset %s",
                    metadata.topic(), metadata.partition(), metadata.offset()));
        } catch (Exception e) {
            // 处理发送消息时可能发生的异常
            return ResponseEntity.status(500).body("Error sending message: " + e.getMessage());
        }
        // 确保生产者关闭，释放资源
    }

    @GetMapping("/readTopic/{topicName}")
    public ResponseEntity<String> readTopic(@PathVariable String topicName) {
        Properties props = new Properties();
        // 添加您的Kafka配置属性
        props.put("bootstrap.servers", "pkc-l6wr6.europe-west2.gcp.confluent.cloud:9092");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='5GECTOGY2ARAL2WS' password='Az7Sjgz57SHqfGYwBA9vE1yfPRTt1iF51OvQcngAn9vok4vw62zF6WarZPQz78Wr';");
        props.put("sasl.mechanism", "PLAIN");
        props.put("group.id", "StockSymbolAnalyzer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        HashMap<String, Double> currentSymbolValueMap = new HashMap<>();
        String[] symbols = {"AAPL", "MSFT", "GOOG", "AMZN", "TSLA"};
        for (String symbol : symbols) {
            currentSymbolValueMap.put(symbol, Double.NaN);
        }

        StringBuilder builder = new StringBuilder();
        int iteration = 0;
        try {
            while (iteration < 100) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    if (currentSymbolValueMap.containsKey(record.key())) {
                        currentSymbolValueMap.put(record.key(), Double.parseDouble(record.value()));
                        builder.append(String.format("[%s] %s: %s %s %s %s%n",
                                record.topic(), record.key(), record.value(),
                                record.partition(), record.offset(), record.timestamp()));
                    } else {
                        builder.append(String.format("The key is: %s, value: %s %n",
                                record.key(), record.value()));
                    }
                }
                iteration++;
            }
        } finally {
            consumer.close(); // 确保在退出循环时关闭消费者
        }

        return ResponseEntity.ok(builder.toString());
    }


    @PostMapping("/readTopic/{topicName}")
    public ResponseEntity<String> readTopicFromPost(@PathVariable String topicName, @RequestBody KafkaPropertiesDTO kafkaProperties) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaProperties.getBootstrapServers());
        props.put("security.protocol", kafkaProperties.getSecurityProtocol());
        props.put("sasl.jaas.config", kafkaProperties.getSaslJaasConfig());
        props.put("sasl.mechanism", kafkaProperties.getSaslMechanism());
        props.put("group.id", kafkaProperties.getGroupId());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        HashMap<String, Double> currentSymbolValueMap = new HashMap<>();
        String[] symbols = {"AAPL", "MSFT", "GOOG", "AMZN", "TSLA"};
        for (String symbol : symbols) {
            currentSymbolValueMap.put(symbol, Double.NaN);
        }

        StringBuilder builder = new StringBuilder();
        int iteration = 0;
        try {
            while (iteration < 100) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    if (currentSymbolValueMap.containsKey(record.key())) {
                        currentSymbolValueMap.put(record.key(), Double.parseDouble(record.value()));
                        builder.append(String.format("[%s] %s: %s %s %s %s%n",
                                record.topic(), record.key(), record.value(),
                                record.partition(), record.offset(), record.timestamp()));
                    } else {
                        builder.append(String.format("The key is: %s, value is: %s %n",
                                record.key(), record.value()));
                    }
                }
                iteration++;
            }
        } finally {
            consumer.close(); // 确保在退出循环时关闭消费者
        }

        return ResponseEntity.ok(builder.toString());
    }

    @PostMapping("/writeTopic/{topicName}/{data}")
    public ResponseEntity<String> writeTopicFromPost(@PathVariable String topicName, @PathVariable String data,@RequestBody KafkaPropertiesDTO kafkaProperties) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaProperties.getBootstrapServers());
        props.put("security.protocol", kafkaProperties.getSecurityProtocol());
        props.put("sasl.jaas.config", kafkaProperties.getSaslJaasConfig());
        props.put("sasl.mechanism", kafkaProperties.getSaslMechanism());
        props.put("group.id", kafkaProperties.getGroupId());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, data);
            // 同步发送消息，并等待响应
            RecordMetadata metadata = producer.send(record).get();
            return ResponseEntity.ok(String.format("Message sent to topic %s: partition: %s with offset: %s",
                    metadata.topic(), metadata.partition(), metadata.offset()));
        } catch (Exception e) {
            // 处理发送消息时可能发生的异常
            return ResponseEntity.status(500).body("Error sending message: " + e.getMessage());
        }
    }

    @PostMapping("transformMessage/{readTopic}/{writeTopic}")
    public ResponseEntity<String> transformMessageFromPost(@PathVariable String readTopic, @PathVariable String writeTopic,@RequestBody KafkaPropertiesDTO kafkaProperties) {
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
                    // 转换数据为大写
                    String originalData = record.value();
                    String transformedData = originalData.toUpperCase();
                    // 写入到writeTopic
                    producer.send(new ProducerRecord<>(writeTopic, transformedData));

                    // 添加转换详情到响应构建器
                    responseBuilder.append(String.format("Value %s from readTopic: %s has transformed into %s to writeTopic: %s.%n",
                            originalData, readTopic, transformedData, writeTopic));
                });

                // 返回所有转换的详情
                return ResponseEntity.ok(responseBuilder.toString());
            } // 自动关闭生产者
        } catch (Exception e) {
            // 异常处理
            return ResponseEntity.status(500).body("Error during message transformation: " + e.getMessage());
        }

}



    public static class KafkaPropertiesDTO {
        private String bootstrapServers;
        private String securityProtocol;
        private String saslJaasConfig;
        private String saslMechanism;
        private String groupId;

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
    }


}
