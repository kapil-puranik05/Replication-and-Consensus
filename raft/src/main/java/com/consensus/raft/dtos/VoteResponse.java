package com.consensus.raft.dtos;

import lombok.Data;

@Data
public class VoteResponse {
    private int voterId;
    private int term;
    private boolean granted;
}
