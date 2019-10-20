package miluroe.datar.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Created by hadoop on 17-6-30.
 */
public class KafkaJavaProducer {
    public final static String TOPIC = "kafka_test";
    public final static String BROKER_LIST = "localhost:9092";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("metadata.broker.list", BROKER_LIST);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        System.out.println("开始生产消息...");
        KafkaProducer<String, String> producer = new KafkaProducer(props);
        while (true) {
            for (int i = 1; i <= 10; i++) {
                producer.send(new ProducerRecord<String, String>(TOPIC, "key-" + i, "message-" + i));
            }
            try {
                Thread.sleep(3000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
