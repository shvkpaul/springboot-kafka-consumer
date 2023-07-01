package com.shvk.libraryeventsconsumer.repository;

import com.shvk.libraryeventsconsumer.model.LibraryEvent;
import org.springframework.data.repository.CrudRepository;

public interface LibraryEventsRepository extends CrudRepository<LibraryEvent,Integer> {
}
