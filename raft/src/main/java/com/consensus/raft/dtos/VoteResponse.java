package com.consensus.raft.dtos;

import lombok.Data;

@Data
public class VoteResponse {
    private String voter;
    private int term;
    private boolean granted;
}
