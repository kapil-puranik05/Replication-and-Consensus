package com.consensus.middleware.dtos;

import lombok.Data;

@Data
public class CommandResponse {
    private boolean accepted;
    private String leaderAddress;
    String message;
}