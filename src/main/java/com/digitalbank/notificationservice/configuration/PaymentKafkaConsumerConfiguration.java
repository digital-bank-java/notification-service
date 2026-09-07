package com.digitalbank.notificationservice.configuration;

import com.digitalbank.notificationservice.adapter.in.kafka.InvalidPaymentEventException;
import com.digitalbank.notificationservice.adapter.in.kafka.PaymentEventKafkaRecoverer;
import com.digitalbank.notificationservice.application.payment.PaymentEventConflictException;
import com.digitalbank.notificationservice.application.payment.PaymentEventQuarantinePort;
import java.util.HashMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@ConditionalOnProperty(name = "notification.events.payment-state.enabled", havingValue = "true")
public class PaymentKafkaConsumerConfiguration {

    @Bean("paymentStateConsumerFactory")
    public ConsumerFactory<String, String> paymentStateConsumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id:notification-service}") String groupId,
            @Value("${spring.kafka.consumer.auto-offset-reset:earliest}") String autoOffsetReset,
            @Value("${spring.kafka.consumer.enable-auto-commit:false}") boolean enableAutoCommit) {
        var properties = new HashMap<String, Object>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(properties);
    }

    @Bean
    public PaymentEventKafkaRecoverer paymentEventKafkaRecoverer(PaymentEventQuarantinePort quarantine) {
        return new PaymentEventKafkaRecoverer(quarantine);
    }

    @Bean("paymentStateKafkaErrorHandler")
    public CommonErrorHandler paymentStateKafkaErrorHandler(PaymentEventKafkaRecoverer recoverer) {
        var errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
        errorHandler.addNotRetryableExceptions(InvalidPaymentEventException.class, PaymentEventConflictException.class);
        return errorHandler;
    }

    @Bean("paymentStateKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> paymentStateKafkaListenerContainerFactory(
            @Qualifier("paymentStateConsumerFactory") ConsumerFactory<String, String> consumerFactory,
            @Qualifier("paymentStateKafkaErrorHandler") CommonErrorHandler errorHandler) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
