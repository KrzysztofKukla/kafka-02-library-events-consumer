package pl.kukla.krzys.kafka02libraryeventsconsumer.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;
import pl.kukla.krzys.kafka02libraryeventsconsumer.domain.LibraryEvent;
import pl.kukla.krzys.kafka02libraryeventsconsumer.repository.LibraryEventRepository;
import pl.kukla.krzys.kafka02libraryeventsconsumer.service.LibraryEventService;

/**
 * @author Krzysztof Kukla
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class LibraryEventServiceImpl implements LibraryEventService {
    private final LibraryEventRepository libraryEventRepository;
    private final ObjectMapper objectMapper;

    @Override
    public void processLibraryEventAndSave(ConsumerRecord<Long, String> consumerRecord) throws JsonProcessingException {
        //value is the libraryEvent message sent by producer
        String message = consumerRecord.value();
        LibraryEvent libraryEvent = objectMapper.readValue(message, LibraryEvent.class);

        //we want to simulate that retry should be call only if RecoverableDataAccessException is thrown
        //not for others Exceptions
        simulateThrowException(libraryEvent.getId());

        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        log.info("Saving libraryEvent: {} to database", libraryEvent.toString());
        libraryEventRepository.save(libraryEvent);

    }

    private void simulateThrowException(Long id) {
        if (id != null) {
            if (id == -1) {
                throw new IllegalArgumentException("id cannot be lower that 0");
            }
            if (id == 0) {
                throw new RecoverableDataAccessException("Temporally Network issue");
            }

        }
    }

}
