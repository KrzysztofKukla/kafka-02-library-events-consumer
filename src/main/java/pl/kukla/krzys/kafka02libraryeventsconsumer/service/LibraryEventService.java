package pl.kukla.krzys.kafka02libraryeventsconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author Krzysztof Kukla
 */
public interface LibraryEventService {
    void processLibraryEventAndSave(ConsumerRecord<Long, String> consumerRecord) throws JsonProcessingException;

}
