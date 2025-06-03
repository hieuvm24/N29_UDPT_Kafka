package chatapp;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;

public class MessageProducer {
    public static void main(String[] args) {
        String topic = "chat-topic";
        
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all"); // Đảm bảo tất cả broker xác nhận
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(props);
             Scanner scanner = new Scanner(System.in)) {
            System.out.println("Nhap tin nhan de gui (go 'exit' de thoat):");

            while (true) {
                System.out.print("> ");
                String message = scanner.nextLine();

                if ("exit".equalsIgnoreCase(message)) break;

                try {
                    producer.send(new ProducerRecord<>(topic, message), (metadata, exception) -> {
                        if (exception == null) {
                            System.out.println("Da gui thanh cong: " + message + " (Partition: " + metadata.partition() + ", Offset: " + metadata.offset() + ")");
                        } else {
                            System.err.println("Loi gui tin nhan: " + exception.getMessage());
                        }
                    });
                    producer.flush(); // Đảm bảo tin nhắn được gửi ngay
                } catch (InterruptException e) {
                    System.err.println("Producer bi ngat ket noi: " + e.getMessage());
                    break;
                }
            }
        } catch (Exception e) {
            System.err.println("Loi khoi tao Producer: " + e.getMessage());
        }
    }
}