package com.consensus.raft.configurations;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

import tools.jackson.databind.ObjectMapper;

@Configuration
public class ExternalConfiguration {
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public ExecutorService executorService() {
        return Executors.newFixedThreadPool(8);
    }

    @Bean
    public WebClient webClient() {
        return WebClient.create();
    }
}
