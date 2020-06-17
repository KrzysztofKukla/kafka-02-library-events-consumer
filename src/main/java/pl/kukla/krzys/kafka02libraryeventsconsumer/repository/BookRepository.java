package pl.kukla.krzys.kafka02libraryeventsconsumer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import pl.kukla.krzys.kafka02libraryeventsconsumer.domain.Book;

/**
 * @author Krzysztof Kukla
 */
public interface BookRepository extends JpaRepository<Book, Long> {
}
