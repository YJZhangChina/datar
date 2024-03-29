package miluroe.datar.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Created by hadoop on 17-6-30.
 */
public class KafkaJavaConsumer {

//    private Consumer consumer;
//    private org.apache.kafka.clients.consumer.KafkaConsumer<String, String> kafkaConsumer;
//    private String topic="kafka_test_4";
//    public KafkaJavaConsumer() {
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
//    }
//    private static ConsumerConfig createConsumerConfig() {
//        Properties props = new Properties();
//        props.put("zookeeper.connect", "localhost:2181");
//        props.put("group.id", "test-consumer-group106");
//        props.put("zookeeper.session.timeout.ms", "5000");
//        props.put("auto.commit.interval.ms", "1000");
//        props.put("rebalance.backoff.ms","3000");
//        props.put("rebalance.max.retries","50");
//        props.put("auto.offset.reset", "smallest");
//        return new ConsumerConfig(props);
//    }
//
//    public void startConsume() {
//        System.out.println("start consume......");
//        Map<String, Integer> topicMap = new HashMap<String, Integer>();
//        ExecutorService threadPool =  Executors.newFixedThreadPool(3);
//        //设置3个线程去消费主题
//        topicMap.put(topic, new Integer(3));
//        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap = consumer.createMessageStreams(topicMap);
//        List<KafkaStream<byte[], byte[]>> streamList = consumerStreamsMap.get(topic);
//        System.out.println("streamList size is : "+streamList.size());
//        int counter = 1;
//        for (final KafkaStream<byte[], byte[]> stream : streamList) {
//            try{
//                threadPool.submit(new Task("consumer_"+counter++,stream));
//            }catch (Exception e){
//                e.printStackTrace();
//            }
//        }
//
//    }
//
//    static class Task implements Runnable{
//
//        private String taskName;
//        private KafkaStream<byte[],byte[]> stream;
//        public Task(String taskName,KafkaStream<byte[], byte[]> stream){
//            this.taskName = taskName;
//            this.stream = stream;
//        }
//
//        @Override
//        public void run() {
//            System.out.println("task "+taskName+" is doing...");
//            ConsumerIterator<byte[], byte[]> it = stream.iterator();
//            while (it.hasNext()){
//                MessageAndMetadata<byte[],byte[]> mes = it.next();
//                System.out.println("task is : "+this.taskName+" ; Topic : "+mes.topic()+"; partition : "+mes.partition()+" ;  message : "+ new String(mes.message()));
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException e) {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                }
//            }
//        }
//    }
//    public static void main(String[] args) {
//
//        KafkaJavaConsumer consumer = new KafkaJavaConsumer();
//        consumer.startConsume();
//    }
}