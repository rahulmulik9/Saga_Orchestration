package com.rahul.paymentservice.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Entity
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "topic")
    private String topic;

    @Lob
    @Column(name = "payload")
    private String payload; // JSON string of the event/command DTO

    @Column(name = "published")
    private boolean published;

    private LocalDateTime createdAt;

    private LocalDateTime publishedAt;
}