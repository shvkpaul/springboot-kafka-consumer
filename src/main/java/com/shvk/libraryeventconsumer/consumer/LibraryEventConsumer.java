package com.shvk.libraryeventconsumer.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaListeners;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventConsumer {

//    @Value("${spring.kafka.topic}")
//    public String topic;

    @KafkaListener(topics = { "library-events" })
    public void onMessage(ConsumerRecord<Integer,String> consumerRecord){
        log.info("ConsumerRecord : {} ",consumerRecord);
    }
}
