package com.shvk.libraryeventsconsumer.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Configuration
//@EnableKafka not needed in the updated version
@Slf4j
public class LibraryEventsConsumerConfig {
    private final KafkaProperties properties;

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;

    public LibraryEventsConsumerConfig(KafkaProperties properties) {
        this.properties = properties;
    }

    public DeadLetterPublishingRecoverer publishingRecoverer() {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate
            , (r, e) -> {
            log.error("Exception in publishingRecoverer : {} ", e.getMessage(), e);
            if (e.getCause() instanceof RecoverableDataAccessException) {
                return new TopicPartition(retryTopic, r.partition());
            } else {
                return new TopicPartition(deadLetterTopic, r.partition());
            }
        }
        );
        return recoverer;
    }

    public DefaultErrorHandler errorHandler() {

        var exceptiopnToIgnorelist = List.of(
            IllegalArgumentException.class
        );

//        var exceptiopnToRetrylist = List.of(
//            RecoverableDataAccessException.class
//        ); //2nd option

        var fixedBackOff = new FixedBackOff(1000L, 2L);

//        ExponentialBackOffWithMaxRetries expBackOff = new ExponentialBackOffWithMaxRetries(2);
//        expBackOff.setInitialInterval(1_000L);
//        expBackOff.setMultiplier(2.0);
//        expBackOff.setMaxInterval(2_000L);

        var errorHandler = new DefaultErrorHandler(
            publishingRecoverer(),
            fixedBackOff
            //expBackOff
        );

        exceptiopnToIgnorelist.forEach(errorHandler::addNotRetryableExceptions);
        //exceptiopnToIgnorelist.forEach(errorHandler::addRetryableExceptions); //2nd option

        errorHandler.setRetryListeners(
            (record, ex, deliveryAttempt) ->
                log.info("Failed Record in Retry Listener  exception : {} , deliveryAttempt : {} ", ex.getMessage(), deliveryAttempt)
        );

        return errorHandler;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
        ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
        ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory,
        ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory
            .getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(this.properties.buildConsumerProperties())));
        //factory.setConcurrency(3); // not necessary if service is running in kubernetes
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        // we will be using default batch commit
        factory.setCommonErrorHandler(errorHandler());// adding custom error handler
        return factory;
    }
}
