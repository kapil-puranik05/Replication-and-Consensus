package com.consensus.raft.dtos;

import lombok.Data;

@Data
public class VoteRequest {
    private String node;
    private int currentTerm;
    private int logLength;
    private int lastTerm;
}
