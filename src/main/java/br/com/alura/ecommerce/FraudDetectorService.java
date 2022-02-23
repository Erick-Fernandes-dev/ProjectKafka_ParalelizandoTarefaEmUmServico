package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class FraudDetectorService {

    public static void main(String[] args) {

        //Criando um consumidor

        var consumer = new KafkaConsumer<String, String>(properties());

        //Escutar algum topico
        consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));
        //OBS: E RARO ESCUTAR MAIS DE UM TOPICO, POIS SE CASO ACONTECER, ISSO TORNA UMA BAGUNCA;

        while (true) {

            var records = consumer.poll(Duration.ofMillis(100));//vai devolver varios registros enviados

            if (!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() + " registros");

                for (var record : records) {

                    System.out.println("---------------------------------------------");
                    System.out.println("Processing new order, checking for fraud");
                    System.out.println(record.key());
                    System.out.println(record.value());
                    System.out.println(record.partition());
                    System.out.println(record.offset());

                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        // ignoring
                        e.printStackTrace();
                    }

                    System.out.println("Order processed");
                }

            }

        }

    }

    private static Properties properties() {

        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // bytes para String --> deserializer
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, FraudDetectorService.class.getSimpleName() + "-" +
                                UUID.randomUUID().toString()); // --> alterar id do cliente
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");// maximo de consumo por vez
        //aqui em cima vai de 1 em 1

        return properties;

    }
}