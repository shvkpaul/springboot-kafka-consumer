package com.shvk.libraryeventsconsumer.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
//@EnableKafka not needed in the updated version
@Slf4j
public class LibraryEventsConsumerConfig {
    private final KafkaProperties properties;

    public LibraryEventsConsumerConfig(KafkaProperties properties) {
        this.properties = properties;
    }

    public DefaultErrorHandler errorHandler() {

        var fixedBackOff = new FixedBackOff(1000L, 2L);

        var errorHandler = new DefaultErrorHandler(
            fixedBackOff
        );

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
        factory.setConcurrency(3); // not necessary if service is running in kubernetes
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        // we will be using default batch commit
        factory.setCommonErrorHandler(errorHandler());// adding custom error handler
        return factory;
    }
}
