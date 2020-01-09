package com.github.anselmoborges.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        // Tipo Variáveis
        String bootstrapServers = "localhost:9092";

        //Cria as propriedades do producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Cria o Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Criando um dado pro producer
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("primeiro_topico", "oieeee!");

        //Envia os dados - Assincrono
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
