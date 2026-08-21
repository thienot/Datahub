package com.example.bhxh_api.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SwaggerConfig {

    @Bean
    public OpenAPI bhxhOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("BHXH Search API")
                        .description("API tra cứu quá trình tham gia BHXH theo mã BHXH hoặc mã người lao động")
                        .version("v1.0"));
    }
}