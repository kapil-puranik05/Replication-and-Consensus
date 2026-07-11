package com.consensus.middleware.controllers;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.consensus.middleware.dtos.Command;
import com.consensus.middleware.dtos.RegistrationRequest;
import com.consensus.middleware.routing.NodeRegistry;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/gateway")
@RequiredArgsConstructor
public class GatewayController {
    private final NodeRegistry nodeRegistry;

    @PostMapping("/register")
    public Mono<String> register(@RequestBody RegistrationRequest request) {
        nodeRegistry.registerNode(request.getAddress());
        return Mono.just("Registered");
    }

    @PostMapping("/write")
    public Mono<ResponseEntity<?>> write(@RequestBody Command command) {
        return Mono.just(new ResponseEntity<>(nodeRegistry.write(command), HttpStatus.ACCEPTED));
    }

    @GetMapping("/{key}")
    public Mono<ResponseEntity<String>> getValue(@PathVariable String key) {
        return nodeRegistry.read(key);
    }
}
