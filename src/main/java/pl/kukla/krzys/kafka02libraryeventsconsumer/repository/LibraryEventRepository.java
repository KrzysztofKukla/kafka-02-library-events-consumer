package pl.kukla.krzys.kafka02libraryeventsconsumer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import pl.kukla.krzys.kafka02libraryeventsconsumer.domain.LibraryEvent;

/**
 * @author Krzysztof Kukla
 */
public interface LibraryEventRepository extends JpaRepository<LibraryEvent, Long> {
}
