package pl.kukla.krzys.kafka02libraryeventsconsumer.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * @author Krzysztof Kukla
 */
@Configuration
//@EnableKafka allows to read all properties for Kafka defined in application.yml
@EnableKafka
public class LibraryEventsConsumerConfig {
}
