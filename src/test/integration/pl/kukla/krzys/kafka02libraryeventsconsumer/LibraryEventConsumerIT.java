package pl.kukla.krzys.kafka02libraryeventsconsumer.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.BDDMockito;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import pl.kukla.krzys.kafka02libraryeventsconsumer.domain.Book;
import pl.kukla.krzys.kafka02libraryeventsconsumer.domain.LibraryEvent;
import pl.kukla.krzys.kafka02libraryeventsconsumer.domain.LibraryEventType;
import pl.kukla.krzys.kafka02libraryeventsconsumer.repository.LibraryEventRepository;
import pl.kukla.krzys.kafka02libraryeventsconsumer.service.LibraryEventService;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author Krzysztof Kukla
 */
@SpringBootTest
//properties for @EmbeddedKafka broker are provided from test/application.yml which overrides /src/application.yml
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
//here we use properties from test/application.yml
public class LibraryEventConsumerIT {

    private static final String BOOK_AUTHOR = "any author";
    private static final String BOOK_NAME = "abcd";
    private static final Long BOOK_ID = null;
    private static final LibraryEventType LIBRARY_EVENT_TYPE = LibraryEventType.NEW;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaTemplate<Long, String> kafkaTemplate;

    @Autowired
    //it holds of all listener containers ( przechowuje wszystkie listenery )
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    //allows access to real Bean
    @SpyBean
    LibraryEventConsumerService libraryEventConsumerServiceSpy;

    @SpyBean
    LibraryEventService libraryEventServiceSpy;

    @Autowired
    LibraryEventRepository libraryEventRepository;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry.getAllListenerContainers()) {
            //we want to be sure, that container which we have here is going to wait until all partitions are assigned to work
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown() {
        libraryEventRepository.deleteAll();
    }

    @Test
    void publishNewLibraryEvent() throws Exception {
        LibraryEvent dummyLibraryEvent = createDummyLibraryEvent(createDummyBook());
        String libraryEventJson = objectMapper.writeValueAsString(dummyLibraryEvent);

        //send to default topic
        //name of default topic is loaded from KafkaAutoConfiguration from application.yml
        //asynchronous call
        //get() method invoke this method synchronous
        kafkaTemplate.sendDefault(libraryEventJson).get();

        //consumer is going to run in different thread from actual application
        //block this Thread until count reach zero and in the meantime ( w miedzyczasie ) consumer will read the message from Kafka topic
        // and process that
        CountDownLatch latch = new CountDownLatch(1);
        //wait max 3 sec
        latch.await(3, TimeUnit.SECONDS);

        BDDMockito.then(libraryEventConsumerServiceSpy).should().onMessage(ArgumentMatchers.any(ConsumerRecord.class));
        BDDMockito.then(libraryEventServiceSpy).should().processLibraryEventAndSave(ArgumentMatchers.any(ConsumerRecord.class));

        Assertions.assertEquals(1, libraryEventRepository.count());

        LibraryEvent libraryEvent = libraryEventRepository.findAll().stream().findFirst().get();
        Assertions.assertAll(
            () -> Assertions.assertNotNull(libraryEvent.getId()),
            () -> Assertions.assertEquals(LibraryEventType.NEW, libraryEvent.getLibraryEventType()),
            () -> {
                Book book = libraryEvent.getBook();
                Assertions.assertNotNull(book);
                Assertions.assertNotNull(book.getId());
                Assertions.assertEquals(BOOK_AUTHOR, book.getAuthor());
                Assertions.assertEquals(BOOK_NAME, book.getName());
            }
        );
    }

    @DisplayName(value = "Retries 3 times regarding to retryPolicy defined in @Configuration")
    @Test
    void publishNewLibraryEventWithRuntimeException() throws Exception {
        //if book is null then throw RuntimeException and consumer retries the reading message
        LibraryEvent dummyLibraryEvent = createDummyLibraryEvent(null);
        String libraryEventJson = objectMapper.writeValueAsString(dummyLibraryEvent);

        //send to default topic
        //name of default topic is loaded from KafkaAutoConfiguration from application.yml
        //asynchronous call
        //get() method invoke this method synchronous
        kafkaTemplate.sendDefault(libraryEventJson).get();

        //consumer is going to run in different thread from actual application
        //block this Thread until count reach zero and in the meantime ( w miedzyczasie ) consumer will read the message from Kafka topic
        // and process that
        CountDownLatch latch = new CountDownLatch(1);
        //wait max 3 sec
        latch.await(3, TimeUnit.SECONDS);

        BDDMockito.then(libraryEventConsumerServiceSpy).should(Mockito.times(3)).onMessage(ArgumentMatchers.any(ConsumerRecord.class));
        BDDMockito.then(libraryEventServiceSpy).should(Mockito.times(3)).processLibraryEventAndSave(ArgumentMatchers.any(ConsumerRecord.class));
    }

    @Test
    void updatePublishLibraryEvent() throws Exception {
        LibraryEvent dummyLibraryEvent = createDummyLibraryEvent(createDummyBook());
        LibraryEvent savedLibraryEvent = libraryEventRepository.save(dummyLibraryEvent);
        Book savedBook = savedLibraryEvent.getBook();

        String bookNameUpdated = "updated book name";
        String bookAuthorUpdated = "updated author of book";
        Book updatedBook = Book.builder()
            .id(savedBook.getId())
            .name(bookNameUpdated)
            .author(bookAuthorUpdated)
            .build();
        savedLibraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        savedLibraryEvent.setBook(updatedBook);
        String libraryEventJson = objectMapper.writeValueAsString(savedLibraryEvent);

        //send to default topic
        //name of default topic is loaded from KafkaAutoConfiguration from application.yml
        //asynchronous call
        //get() method invoke this method synchronous
        kafkaTemplate.sendDefault(savedLibraryEvent.getId(), libraryEventJson).get();

        //consumer is going to run in different thread from actual application
        //block this Thread until count reach zero and in the meantime ( w miedzyczasie ) consumer will read the message from Kafka topic
        // and process that
        CountDownLatch latch = new CountDownLatch(1);
        //wait max 3 sec
        latch.await(3, TimeUnit.SECONDS);

        BDDMockito.then(libraryEventConsumerServiceSpy).should().onMessage(ArgumentMatchers.any(ConsumerRecord.class));
        BDDMockito.then(libraryEventServiceSpy).should().processLibraryEventAndSave(ArgumentMatchers.any(ConsumerRecord.class));

        Optional<LibraryEvent> libraryEventOptional = libraryEventRepository.findById(savedLibraryEvent.getId());
        Assertions.assertTrue(libraryEventOptional.isPresent());
        LibraryEvent libraryEventReadFromKafka = libraryEventOptional.get();
        Assertions.assertAll(
            () -> Assertions.assertEquals(savedLibraryEvent.getId(), libraryEventReadFromKafka.getId()),
            () -> Assertions.assertEquals(LibraryEventType.UPDATE, libraryEventReadFromKafka.getLibraryEventType()),
            () -> {
                Book book = libraryEventReadFromKafka.getBook();
                Assertions.assertNotNull(book);
                Assertions.assertNotNull(book.getId());
                Assertions.assertEquals(savedLibraryEvent.getBook().getId(), book.getId());
                Assertions.assertEquals(bookNameUpdated, book.getName());
                Assertions.assertEquals(bookAuthorUpdated, book.getAuthor());
            }
        );

    }

    private LibraryEvent createDummyLibraryEvent(Book book) {
        return LibraryEvent.builder()
            .libraryEventType(LIBRARY_EVENT_TYPE)
            .book(book)
            .build();
    }

    private Book createDummyBook() {
        return Book.builder()
            .id(BOOK_ID)
            .author(BOOK_AUTHOR)
            .name(BOOK_NAME)
            .build();
    }

}
