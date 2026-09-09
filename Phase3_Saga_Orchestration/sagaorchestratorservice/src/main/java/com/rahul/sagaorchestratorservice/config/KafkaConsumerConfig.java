package com.rahul.sagaorchestratorservice.config;

import com.rahul.sagaorchestratorservice.dto.inventory.InventoryRejected;
import com.rahul.sagaorchestratorservice.dto.inventory.InventoryReleased;
import com.rahul.sagaorchestratorservice.dto.inventory.InventoryReserved;
import com.rahul.sagaorchestratorservice.dto.payment.PaymentCompleted;
import com.rahul.sagaorchestratorservice.dto.payment.PaymentFailed;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import com.rahul.sagaorchestratorservice.dto.order.OrderCreated;

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
    public ConcurrentKafkaListenerContainerFactory<String, OrderCreated> orderCreatedContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OrderCreated> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("sagaorchestratorservice-order-created-group", OrderCreated.class));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String,InventoryReserved> inventoryReservedContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, InventoryReserved> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("sagaorchestratorservice-inventory-reserved-group",InventoryReserved.class));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, InventoryRejected> inventoryRejectedContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, InventoryRejected> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("sagaorchestratorservice-inventory-rejected-group", InventoryRejected.class));
        return factory;
    }



    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentCompleted> paymentCompletedContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, PaymentCompleted> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("sagaorchestratorservice-payment-completed-group", PaymentCompleted.class));
        return factory;
    }


    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentFailed> paymentFailedContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, PaymentFailed> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("sagaorchestratorservice-payment-failed-group", PaymentFailed.class));
        return factory;
    }


    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, InventoryReleased> inventoryReleasedContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, InventoryReleased> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("sagaorchestratorservice-inventory-released-group", InventoryReleased.class));
        return factory;
    }
}