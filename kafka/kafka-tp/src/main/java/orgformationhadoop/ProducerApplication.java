package orgformationhadoop;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerApplication {

    private KafkaProducer<String, String> producer;

    // Méthode pour initialiser la configuration
    public void setup() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(props);
    }

    // Méthode pour envoyer un message sur un topic
    public void sendMessage(String topic, String message) {
        producer.send(new ProducerRecord<>(topic, message));
    }

    // Méthode main pour tester
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java ProducerApplication <message>");
            System.exit(1);
        }

        String topicName = "topicname";
        String message = args[0];

        // Instancier la classe ProducerApplication
        ProducerApplication producerApp = new ProducerApplication();

        // Appeler la méthode setup
        producerApp.setup();

        // Envoyer un message vers le topic
        producerApp.sendMessage(topicName, message);

        // Fermer le producer
        producerApp.producer.close();
    }
}
