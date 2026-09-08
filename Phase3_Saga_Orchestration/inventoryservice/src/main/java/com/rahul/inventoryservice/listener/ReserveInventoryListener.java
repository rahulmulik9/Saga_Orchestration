package com.rahul.inventoryservice.listener;

import com.rahul.inventoryservice.dto.sagaDto.KafkaTopics;
import com.rahul.inventoryservice.dto.sagaDto.ReserveInventoryCommand;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ReserveInventoryListener {

    @KafkaListener(
            topics = KafkaTopics.INVENTORY_RESERVE,
            containerFactory = "reserveInventoryContainerFactory"
    )
    public void handle(ReserveInventoryCommand command) {
        log.info("Received ReserveInventoryCommand for orderId={}, items={}",
                command.getOrderId(), command.getItems());
        // stock deduction logic comes in Step 4.2
    }
}