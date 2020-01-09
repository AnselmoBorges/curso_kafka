package com.github.anselmoborges.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Tipo Variáveis
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

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

            String topic = "primeiro_topico";
            String value = "Testando denovo o nro " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key); //Loga para onde a key vai

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
            }).get(); //Força fazer sincrono assim o key é exibido junto com o envio, caso contrario ele mostra os keys primeiro (Assincrono)
        }
        producer.flush();
        producer.close();
    }
}