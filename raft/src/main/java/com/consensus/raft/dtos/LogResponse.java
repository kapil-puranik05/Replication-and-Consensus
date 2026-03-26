package com.consensus.raft.dtos;

import lombok.Data;

@Data
public class LogResponse {
    private int nodeId;
    private int term;
    private int ackedLength;
    private boolean acknowledged;
}
