package com.shvk.libraryeventsconsumer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class AutoCreateConfig {

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;

    @Bean
    public NewTopic retryTopic() {

        return TopicBuilder
                .name(retryTopic)
                .partitions(3)
                .replicas(3)
                .build();

    }

    @Bean
    public NewTopic deadLetterTopic() {

        return TopicBuilder
            .name(deadLetterTopic)
            .partitions(3)
            .replicas(3)
            .build();

    }
}
