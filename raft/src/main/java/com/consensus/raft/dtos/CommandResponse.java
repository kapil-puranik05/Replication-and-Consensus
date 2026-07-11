package com.consensus.raft.dtos;

import lombok.Data;

@Data
public class CommandResponse {
    private boolean accepted;
    private String leaderAddress;
    String message;
}