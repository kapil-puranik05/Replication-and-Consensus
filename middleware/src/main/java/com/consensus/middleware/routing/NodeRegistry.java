package com.consensus.middleware.routing;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.CopyOnWriteArrayList;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import com.consensus.middleware.dtos.Command;
import com.consensus.middleware.dtos.CommandResponse;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class NodeRegistry {
    private NodeDetails nodeDetails;
    private final WebClient webClient;

    @PostConstruct
    public void init() {
        nodeDetails = new NodeDetails();
        nodeDetails.setRegistry(new CopyOnWriteArrayList<>());
    }

    public void registerNode(String address) {
        if (!nodeDetails.getRegistry().contains(address)) {
            nodeDetails.getRegistry().add(address);
        }
    }

    public Mono<String> discoverAndSetLeader() {
        return Flux.fromIterable(nodeDetails.getRegistry())
                .concatMap(nodeAddress -> webClient.get()
                        .uri("http://" + nodeAddress + "/state")
                        .retrieve()
                        .bodyToMono(Map.class)
                        .timeout(Duration.ofSeconds(5))
                        .onErrorResume(TimeoutException.class, e -> Mono.empty())
                        .onErrorResume(Exception.class, e -> Mono.empty()))
                .map(responseMap -> {
                    if (responseMap != null && responseMap.containsKey("leader")) {
                        return (String) responseMap.get("leader");
                    }
                    return null;
                })
                .filter(leader -> leader != null && !leader.isEmpty())
                .next()
                .doOnNext(leader -> {
                    nodeDetails.setLeaderAddress(leader);
                    System.out.println("Leader discovered and set: " + leader);
                });
    }

    public Mono<String> getCurrentLeader() {
        String leader = nodeDetails.getLeaderAddress();
        return leader != null && !leader.isEmpty() ? Mono.just(leader) : Mono.empty();
    }

    public Mono<Boolean> write(Command command) {
        return getCurrentLeader()
                .switchIfEmpty(discoverAndSetLeader())
                .flatMap(leader -> forwardWrite(leader, command))
                .onErrorResume(e -> discoverAndSetLeader()
                        .flatMap(leader -> forwardWrite(leader, command)))
                .map(response -> response.isAccepted());
    }

    private Mono<CommandResponse> forwardWrite(String leader, Command command) {
        return webClient.post()
                .uri("http://" + leader + "/command")
                .bodyValue(command)
                .retrieve()
                .bodyToMono(CommandResponse.class);
    }

    public Mono<ResponseEntity<String>> read(String key) {
        return getCurrentLeader()
                .switchIfEmpty(discoverAndSetLeader())
                .flatMap(leader -> forwardRead(leader, key))
                .onErrorResume(e -> {
                    nodeDetails.setLeaderAddress(null);
                    return discoverAndSetLeader()
                            .flatMap(leader -> forwardRead(leader, key));
                });
    }

    private Mono<ResponseEntity<String>> forwardRead(String leader, String key) {

        return webClient.get()
                .uri("http://" + leader + "/" + key)
                .exchangeToMono(response -> {

                    if (response.statusCode().is2xxSuccessful()) {
                        return response.bodyToMono(String.class)
                                .map(ResponseEntity::ok);
                    }

                    if (response.statusCode().value() == 404) {
                        return response.bodyToMono(String.class)
                                .map(body -> ResponseEntity.status(404).body(body));
                    }

                    return response.createException()
                            .flatMap(Mono::error);
                });
    }
}