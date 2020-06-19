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
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

/**
 * @author Krzysztof Kukla
 */
@Configuration
//@EnableKafka allows to read all properties for Kafka defined in application.yml
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    //from KafkaAnnotationDrivenConfiguration
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

        //custom handler if retry is failed im. temporally network issue with database which cause RuntimeException in service layer
        factory.setRetryTemplate(retryTemplate());
        return factory;
    }

    private RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(createRetryPolicy());
        retryTemplate.setBackOffPolicy(createBackOfRetry());
        return retryTemplate;
    }

    private BackOffPolicy createBackOfRetry() {
        //invoke before each retry backOff ( odsunięcie, opóźnienie )
        FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
        backOffPolicy.setBackOffPeriod(1000);
        return backOffPolicy;
    }

    private RetryPolicy createRetryPolicy() {
        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();

        //if any record is failed then is going to retries max attempts for each record
        simpleRetryPolicy.setMaxAttempts(3);
        return simpleRetryPolicy;
    }

}
