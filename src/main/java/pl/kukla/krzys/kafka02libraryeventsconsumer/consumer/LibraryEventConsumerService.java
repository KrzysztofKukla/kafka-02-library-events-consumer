package pl.kukla.krzys.kafka02libraryeventsconsumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author Krzysztof Kukla
 */
public interface LibraryEventConsumerService {
    void onMessage(ConsumerRecord<Long, String> consumerRecord) throws JsonProcessingException;

}
