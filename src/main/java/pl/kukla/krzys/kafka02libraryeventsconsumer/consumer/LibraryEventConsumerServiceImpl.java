package pl.kukla.krzys.kafka02libraryeventsconsumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import pl.kukla.krzys.kafka02libraryeventsconsumer.service.LibraryEventService;

/**
 * @author Krzysztof Kukla
 */
//disable to demonstrate LibraryEventConsumerManualOffset
@Service
@Slf4j
@RequiredArgsConstructor
public class LibraryEventConsumerServiceImpl implements LibraryEventConsumerService {

    private static final String TOPIC = "library-events";

    private final LibraryEventService libraryEventService;

    @Override
    @KafkaListener(topics = {TOPIC})
    public void onMessage(ConsumerRecord<Long, String> consumerRecord) throws JsonProcessingException {
        log.info("Kafka listener reads from {} topic and consumerRecord is: {}", consumerRecord.topic(), consumerRecord);

        libraryEventService.processLibraryEventAndSave(consumerRecord);
    }

}
