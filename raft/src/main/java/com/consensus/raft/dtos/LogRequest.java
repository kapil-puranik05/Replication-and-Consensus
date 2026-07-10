package com.consensus.raft.dtos;

import java.util.List;

import lombok.Data;

@Data
public class LogRequest {
    private String leader;
    private int term;
    private int prefixLength;
    private int prefixTerm;
    private int commitLength;
    private List<LogEntry> suffix;
}
