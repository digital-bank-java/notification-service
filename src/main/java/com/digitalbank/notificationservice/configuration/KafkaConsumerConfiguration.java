package com.digitalbank.notificationservice.configuration;

import com.digitalbank.notificationservice.adapter.in.kafka.InvalidTransferEventException;
import com.digitalbank.notificationservice.application.event.TransferEventConflictException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@ConditionalOnProperty(name = "notification.events.transfer-created.enabled", havingValue = "true")
public class KafkaConsumerConfiguration {

    @Bean
    public CommonErrorHandler transferCreatedKafkaErrorHandler() {
        var errorHandler = new DefaultErrorHandler(new FixedBackOff(1000L, 2L));
        errorHandler.addNotRetryableExceptions(
                InvalidTransferEventException.class, TransferEventConflictException.class);
        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> transferCreatedKafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory, CommonErrorHandler transferCreatedKafkaErrorHandler) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(transferCreatedKafkaErrorHandler);
        return factory;
    }
}
