package com.consensus.raft.dtos;

import lombok.Data;

@Data
public class VoteRequest {
    private int nodeId;
    private int currentTerm;
    private int logLength;
    private int lastTerm;
}
