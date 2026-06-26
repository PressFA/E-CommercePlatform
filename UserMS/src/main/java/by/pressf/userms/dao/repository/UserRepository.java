package by.pressf.userms.dao.repository;

import by.pressf.userms.dao.entity.UserEntity;
import org.jspecify.annotations.NullMarked;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@NullMarked @Repository
public interface UserRepository extends JpaRepository<UserEntity, UUID> {
}
