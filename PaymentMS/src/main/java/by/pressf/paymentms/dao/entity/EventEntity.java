package by.pressf.paymentms.dao.entity;

import by.pressf.core.entities.BaseEventEntity;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@NoArgsConstructor
@Entity @Table(name = "processed_messages")
public class EventEntity extends BaseEventEntity { }
