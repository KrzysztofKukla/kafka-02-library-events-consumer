package integration.pl.kukla.krzys.kafka02libraryeventsconsumer.consumer;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;

/**
 * @author Krzysztof Kukla
 */
@SpringBootTest
//properties for @EmbeddedKafka broker are provided from test/application.yml which overrides /src/application.yml
@EmbeddedKafka
//here we use properties from test/application.yml
public class LibraryEventConsumerIT {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaTemplate<Long, String> kafkaTemplate;

    @Autowired
    //it holds of all listener containers ( przechowuje wszystkie listenery )
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @BeforeEach
    void setUp() {
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry.getAllListenerContainers()) {
            //we want to be sure, that container which we have here is going to wait until all partitions are assigned to work
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

}
