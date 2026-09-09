package com.rahul.orderservice.config;

import com.rahul.orderservice.dto.sagaDto.ConfirmOrderCommand;
import com.rahul.orderservice.dto.sagaDto.OrderFailedCommand;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private <T> ConsumerFactory<String, T> consumerFactory(String groupId, Class<T> targetType) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
        props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JacksonJsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, targetType.getName());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ConfirmOrderCommand> confirmOrderContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, ConfirmOrderCommand> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("orderservice-confirm-group", ConfirmOrderCommand.class));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderFailedCommand> orderFailedContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OrderFailedCommand> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("orderservice-failed-group", OrderFailedCommand.class));
        return factory;
    }
}