package com.shvk.libraryeventsconsumer.unit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shvk.libraryeventsconsumer.model.Book;
import com.shvk.libraryeventsconsumer.model.LibraryEvent;
import com.shvk.libraryeventsconsumer.model.LibraryEventType;
import com.shvk.libraryeventsconsumer.repository.LibraryEventsRepository;
import com.shvk.libraryeventsconsumer.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LibraryEventsServiceUnitTest {

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Mock
    private LibraryEventsRepository libraryEventsRepository;

    @InjectMocks
    private LibraryEventsService libraryEventsService;

    @Captor
    private ArgumentCaptor<LibraryEvent> libraryEventCaptor;

    @Test
    public void givenNewEvent_whenProcessLibraryEvent_thenSaveCalled() throws JsonProcessingException {
        // Given
        String jsonEvent = "{\"libraryEventType\":\"NEW\",\"book\":{\"libraryEvent\":null}}";
        LibraryEvent libraryEvent = new LibraryEvent();
        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        libraryEvent.setBook(new Book());

        ConsumerRecord<Integer, String> consumerRecord = new ConsumerRecord<>("topic", 1, 1, 1, jsonEvent);

        when(objectMapper.readValue(jsonEvent, LibraryEvent.class)).thenReturn(libraryEvent);

        // When
        libraryEventsService.processLibraryEvent(consumerRecord);

        // Then
        verify(libraryEventsRepository).save(libraryEventCaptor.capture());
        LibraryEvent capturedEvent = libraryEventCaptor.getValue();
        assertEquals(LibraryEventType.NEW, capturedEvent.getLibraryEventType());
        assertNotNull(capturedEvent.getBook().getLibraryEvent());
    }

    @Test
    public void givenUpdateEventWithValidId_whenProcessLibraryEvent_thenSaveCalled() throws JsonProcessingException {
        // Given
        String jsonEvent = "{\"libraryEventType\":\"UPDATE\",\"libraryEventId\":123,\"book\":{\"libraryEvent\":null}}";
        LibraryEvent libraryEvent = new LibraryEvent();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setLibraryEventId(123);
        libraryEvent.setBook(new Book());

        ConsumerRecord<Integer, String> consumerRecord = new ConsumerRecord<>("topic", 1, 1, 1, jsonEvent);

        when(objectMapper.readValue(jsonEvent, LibraryEvent.class)).thenReturn(libraryEvent);
        when(libraryEventsRepository.findById(123)).thenReturn(Optional.of(libraryEvent));

        // When
        libraryEventsService.processLibraryEvent(consumerRecord);

        // Then
        verify(libraryEventsRepository).save(libraryEventCaptor.capture());
        LibraryEvent capturedEvent = libraryEventCaptor.getValue();
        assertEquals(LibraryEventType.UPDATE, capturedEvent.getLibraryEventType());
        assertNotNull(capturedEvent.getBook().getLibraryEvent());
    }

    @Test
    public void givenUpdateEventWithMissingId_whenProcessLibraryEvent_thenExceptionThrown() throws JsonProcessingException {
        // Given
        String jsonEvent = "{\"libraryEventType\":\"UPDATE\",\"book\":{\"libraryEvent\":null}}";
        LibraryEvent libraryEvent = new LibraryEvent();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(new Book());

        ConsumerRecord<Integer, String> consumerRecord = new ConsumerRecord<>("topic", 1, 1, 1, jsonEvent);

        when(objectMapper.readValue(jsonEvent, LibraryEvent.class)).thenReturn(libraryEvent);

        // When/Then
        assertThrows(IllegalArgumentException.class, () -> libraryEventsService.processLibraryEvent(consumerRecord));
        verify(libraryEventsRepository, never()).save(any());
    }

    @Test
    public void givenInvalidEventType_whenProcessLibraryEvent_thenLogErrorMessage() throws JsonProcessingException {
        // Given
        String jsonEvent = "{\"libraryEventType\":\"INVALID\",\"libraryEventId\":123,\"book\":{\"libraryEvent\":null}}";
        LibraryEvent libraryEvent = new LibraryEvent();
        libraryEvent.setLibraryEventType(LibraryEventType.INVAILD);
        libraryEvent.setLibraryEventId(123);
        libraryEvent.setBook(new Book());

        ConsumerRecord<Integer, String> consumerRecord = new ConsumerRecord<>("topic", 1, 1, 1, jsonEvent);

        when(objectMapper.readValue(jsonEvent, LibraryEvent.class)).thenReturn(libraryEvent);

        // When
        libraryEventsService.processLibraryEvent(consumerRecord);

        // Then
        verify(libraryEventsRepository, never()).save(any());
    }

    @Test
    public void givenLibraryEventWithNetworkIssue_whenProcessLibraryEvent_thenRetry() throws JsonProcessingException {
        // Given
        String jsonEvent = "{\"libraryEventType\":\"UPDATE\",\"libraryEventId\":999,\"book\":{\"libraryEvent\":null}}";
        LibraryEvent libraryEvent = new LibraryEvent();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setLibraryEventId(999);
        libraryEvent.setBook(new Book());

        ConsumerRecord<Integer, String> consumerRecord = new ConsumerRecord<>("topic", 1, 1, 1, jsonEvent);

        when(objectMapper.readValue(jsonEvent, LibraryEvent.class)).thenReturn(libraryEvent);

        // When
        assertThrows(RecoverableDataAccessException.class, () -> libraryEventsService.processLibraryEvent(consumerRecord));

        // Then
        verify(libraryEventsRepository, never()).save(any());
    }
}