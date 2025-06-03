package chatapp;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class MessageConsumer {
    public static void main(String[] args) {
        String topic = "chat-topic";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "chat-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest"); // Đọc từ đầu topic nếu chưa có offset

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("Dang cho tin nhan... (Nhan Ctrl+C de thoat)");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Tin nhan nhan duoc: %s (Partition: %d, Offset: %d)%n",
                            record.value(), record.partition(), record.offset());
                }
            }
        } catch (Exception e) {
            System.err.println("Loi khoi tao hoac ket noi Consumer: " + e.getMessage());
        }
    }
}