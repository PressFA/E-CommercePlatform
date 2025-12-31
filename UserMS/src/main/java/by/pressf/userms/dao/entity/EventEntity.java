package by.pressf.userms.dao.entity;

import by.pressf.core.entity.BaseEventEntity;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@Entity @Table(name = "processed_messages")
public class EventEntity extends BaseEventEntity { }
