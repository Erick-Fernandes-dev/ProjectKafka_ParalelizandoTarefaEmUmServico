package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // criando um produtor
        //                               chave   valor
        var producer = new KafkaProducer<String, String>(properties());

        for (int i = 0; i < 100; i++) {

            var key = UUID.randomUUID().toString();

            var value = key + ", 1346, 7900,00";

            var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key, value);
            Callback callback = (data, ex) -> {

                if (ex != null) {
                    ex.printStackTrace();
                    return;
                }
                System.out.println("Sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset " +
                        data.offset() + "/ timestamp " + data.timestamp());
            };

            var email = "thank you for your order! We are processing your order!";
            var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);

            producer.send(record, callback).get();
            producer.send(emailRecord, callback).get();

            // record --> registro
        }
    }

    private static Properties properties() {

        var properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;

    }

    /*
    *== Serializar String em bytes ============================
    *
    * KEY_SERIALIZER_CLASS_CONFIG --> Serializador de Strings
    * VALUE_SERIALIZER_CLASS_CONFIG --> Mensagens
    *
    *==========================================================
    *
    *
    * */
}