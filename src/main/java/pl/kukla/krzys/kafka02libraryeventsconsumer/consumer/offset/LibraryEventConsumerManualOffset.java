package pl.kukla.krzys.kafka02libraryeventsconsumer.consumer.offset;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

/**
 * @author Krzysztof Kukla
 */
//@Service
@Slf4j
public class LibraryEventConsumerManualOffset implements AcknowledgingMessageListener<Long, String> {

    public static final String TOPIC = "library-events";

    @Override
    @KafkaListener(topics = {TOPIC})
    public void onMessage(ConsumerRecord<Long, String> consumerRecord, Acknowledgment acknowledgment) {
        log.info("Kafka listener read from {} topic and consumerRecord is: {}", consumerRecord.topic(), consumerRecord);

        //it lets Listener knows the message has been processed successfully
        // - we want to manually manage the offsets - commit offset on demand
        acknowledgment.acknowledge();
    }

}
