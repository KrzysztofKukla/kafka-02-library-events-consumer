package pl.kukla.krzys.kafka02libraryeventsconsumer.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

/**
 * @author Krzysztof Kukla
 */
@Configuration
//@EnableKafka allows to read all properties for Kafka defined in application.yml
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
                                                                                ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory();
        ContainerProperties containerProperties = factory.getContainerProperties();
        configurer.configure(factory, kafkaConsumerFactory.getIfAvailable());

        //here we've changed default ActMode.BATCH ( default committing processed offsets ) to MANUAL
        // - we want to manually manage the offsets - commit offset on demand
//        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        //tree separate instance in 3 threads ( 3 separate KafkaListeners for the same application) - recommended only if we are running application
        // NON  in cloud environment
        //for Cloud or Kubernetes this option is not necessary
        //each separate thread ( listener ) is going to read message from separate partition from topic ( we can see it on console logs )
        factory.setConcurrency(3);

        //handle custom error to provide any special details information
        factory.setErrorHandler(((thrownException, data) -> {
            log.error("Exception in consumerConfig is {} and record is {}", thrownException.getMessage(), data);
        }));
        return factory;
    }

}
