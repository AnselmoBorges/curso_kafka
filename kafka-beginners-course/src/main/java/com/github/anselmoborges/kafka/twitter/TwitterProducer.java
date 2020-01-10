package com.github.anselmoborges.kafka.twitter;

// Importações de modulos
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    /** Variaveis de autenticação **/
    String consumerKey = "ipVqi6oDHcBkqRqIyvmXud6OJ";
    String consumerSecret = "73hqSqmX2b1rv0rJDgbwyRBRLNfFNS3MxwNciOFuVd75lNswyF";
    String token = "1149790189256097792-yT2m8m2MyYlII9HVuj1l3RIoRigHX5";
    String secret = "0ECKJxrmwmeMkTlpldIvYUl2YIdWsaGZM8OljtD4pagio";
    List<String> terms = Lists.newArrayList("Rastreador Ituran", "Custom Audio");

    public TwitterProducer(){}

    public static void main(String[] args) {
        new TwitterProducer().run();
    }
    public void run(){

        logger.info("Setup");

        // Configurando o Message Queue
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // Criando um client do Twitter
        Client client = createTwitterClient(msgQueue);
        // Estabelecendo uma conexão com o Twitter
        client.connect();

        // Criando um producer do Kafka
        KafkaProducer<String, String> producer = createKafkaProducer();

        // Loop para enviar os Tweets para a fila do Kafka.
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null){
                logger.info(msg);
                producer.send(new ProducerRecord<>("tweets_spc", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Alguma coisa muito ruim aconteceu! =(");
                        }
                    }
                });
            }
        }
        logger.info("Fim da aplicação!");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        // Onde vamos nos conectart de forma autenticada ou não
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(terms);

        // Configuração das chaves fornecidas pelo Twitter Developer
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        BlockingQueue<Event> eventQueue;
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
    public KafkaProducer<String, String> createKafkaProducer(){
        // Tipo Variáveis
        String bootstrapServers = "localhost:9092";

        //Cria as propriedades do producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Cria o Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}