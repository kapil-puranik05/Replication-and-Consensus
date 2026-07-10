package com.consensus.raft.dtos;

import lombok.Data;

@Data
public class LogResponse {
    private String node;
    private int term;
    private int ackedLength;
    private boolean acknowledged;
}
