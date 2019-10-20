package miluroe.datar.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaSingletonProducer {

    public static final String brokerList = "localhost:9092";

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokerList);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("client.id", "producer.client.id.demo");
        return properties;
    }

    public static final String topic = "topic-demo";
//    private  final static String TOPIC = "index_name";
//    private  final static String FILENAME = "baidu.txt";


    public static void main(String[] args) {
        Properties properties = initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "Hello, Kafka!");
        try {
            producer.send(record);
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
//        String path = "data/"+FILENAME;
//        File file = new File(path);
////        StringBuilder result = new StringBuilder();
//
//        Properties props = new Properties();
//        // 服务器ip:端口号，集群用逗号分隔
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigLoader.load_value("application.properties","kafka_cluster"));
//        props.put("acks", "all");
//        // key序列化指定类
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        // value序列化指定类
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//
//        // 生产者对象
//        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
//        // 向first发送hello, kafka
//        int length = 0;
//        try{
//            BufferedReader br = new BufferedReader(new FileReader(file));//构造一个BufferedReader类来读取文件
//            String s = null;
//            while((s = br.readLine())!=null){//使用readLine方法，一次读一行
//                producer.send(new ProducerRecord<String, String>(TOPIC, s));
////                System.out.println("一条记录发送成功");
//                length++;
//            }
//            br.close();
//            producer.close();
//        }catch(Exception e){
//            e.printStackTrace();
//        }
//        System.out.println("共"+length+"行数据");

    }
}
