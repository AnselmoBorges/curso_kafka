package com.github.anselmoborges.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Properties;

public class ProducerDemoComCallback {

    public static void main(String[] args) {
        // Tipo Variáveis
        final Logger logger = LoggerFactory.getLogger(ProducerDemoComCallback.class);

        String bootstrapServers = "localhost:9092";

        //Cria as propriedades do producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Cria o Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=0; i<10; i++){
            // Criando um dado pro producer
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("primeiro_topico", "vamos ver! " + Integer.toString(i));

            //Envia os dados - Assincrono
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Executa todas as vezes que um dado for enviado com sucesso ao Kafka ou cria uma exception se falha
                    if (e == null) {
                        // O registro é enviado com sucesso!
                        logger.info("Dado recebido. \n" + "Topic:" + recordMetadata.topic() + "\n" + "Partition:" + recordMetadata.partition() + "\n" + "Offset:" + recordMetadata.offset() + "\n" + "Timestamp:" + recordMetadata.timestamp());
                    } else {
                        logger.error("Erro no envio do registro", e);
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }
}