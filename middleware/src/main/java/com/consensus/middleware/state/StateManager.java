package com.consensus.middleware.state;

import java.util.concurrent.atomic.AtomicReference;

import org.springframework.stereotype.Component;

@Component
public class StateManager {
    private AtomicReference<String> state = new AtomicReference<>();

    public void set(String value) {
        state.set(value);
    }

    public String get() {
        return state.get();
    }
}
