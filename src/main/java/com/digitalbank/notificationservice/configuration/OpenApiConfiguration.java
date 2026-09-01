package com.digitalbank.notificationservice.configuration;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfiguration {

    @Bean
    OpenAPI notificationServiceOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("Digital Bank Notification Service API")
                        .version("1.0.0")
                        .description("Internal notification delivery API for the Digital Bank Java platform."));
    }
}
