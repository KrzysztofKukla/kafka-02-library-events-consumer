package pl.kukla.krzys.kafka02libraryeventsconsumer.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * @author Krzysztof Kukla
 */
//disable to demonstrate LibraryEventConsumerManualOffset
@Service
@Slf4j
public class LibraryEventConsumerServiceImpl implements LibraryEventConsumerService {

    public static final String TOPIC = "library-events";

    @Override
    @KafkaListener(topics = {TOPIC})
    public void onMessage(ConsumerRecord<Long, String> consumerRecord) {
        log.info("Kafka listener read from {} topic and consumerRecord is: {}", consumerRecord.topic(), consumerRecord);
    }

}
