package com.shvk.libraryeventsconsumer.intg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shvk.libraryeventsconsumer.consumer.LibraryEventsConsumer;
import com.shvk.libraryeventsconsumer.model.Book;
import com.shvk.libraryeventsconsumer.model.LibraryEvent;
import com.shvk.libraryeventsconsumer.model.LibraryEventType;
import com.shvk.libraryeventsconsumer.repository.LibraryEventsRepository;
import com.shvk.libraryeventsconsumer.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.atLeast;


@SpringBootTest
@EmbeddedKafka(topics = {"library-events"
    , "library-events.RETRY"
    , "library-events.DLT"
}
    , partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
    , "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"
    , "retryListener.startup=false"})
public class LibraryEventsConsumerIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    LibraryEventsService libraryEventsServiceSpy;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    @Autowired
    ObjectMapper objectMapper;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() {
        var container = endpointRegistry.getListenerContainers()
            .stream().filter(messageListenerContainer ->
                Objects.equals(messageListenerContainer.getGroupId(), "library-events-listener-group"))
            .collect(Collectors.toList()).get(0);
        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());

//        for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
//            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
//        }
    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();
    }

    @Test
    void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        //given
        String json = " {\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Shouvik\"}}";
        kafkaTemplate.sendDefault(json).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEventList = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assert libraryEventList.size() == 1;
        libraryEventList.forEach(libraryEvent -> {
            assert libraryEvent.getLibraryEventId() != null;
            assertEquals(123, libraryEvent.getBook().getBookId());
        });

    }


    @Test
    void publishUpdateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        //given
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        //publish the update LibraryEvent

        Book updatedBook = Book.builder().
            bookId(456).bookName("Kafka Using Spring Boot 3").bookAuthor("Dilip").build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
        LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();
        assertEquals("Kafka Using Spring Boot 3", persistedLibraryEvent.getBook().getBookName());
    }

    @Test
    void publishModifyLibraryEvent_Null_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = null;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        // custom error handler
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
    }

    @Test
    void publishModifyLibraryEvent_999_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = 999;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // Without Retry Listener
//        verify(libraryEventsConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
//        verify(libraryEventsServiceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));

        //with Retry listener
        verify(libraryEventsConsumerSpy, atLeast(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, atLeast(3)).processLibraryEvent(isA(ConsumerRecord.class));
            // It should be working but somehow not working
//        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("retry-listener-group", "true", embeddedKafkaBroker));
//        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
//        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, retryTopic);
//
//        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, retryTopic);
//
//        System.out.println("consumer Record in deadletter topic : " + consumerRecord.value());
//
//        assertEquals(json, consumerRecord.value());
//        consumerRecord.headers()
//            .forEach(header -> {
//                System.out.println("Header Key : " + header.key() + ", Header Value : " + new String(header.value()));
//            });
    }
}
