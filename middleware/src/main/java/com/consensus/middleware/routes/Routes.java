package com.consensus.middleware.routes;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;

@Data
@Component
@ConfigurationProperties(prefix = "cluster")
public class Routes {
    private String route1;
    private String route2;
    private String route3;
}
