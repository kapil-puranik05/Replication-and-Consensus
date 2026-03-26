package com.consensus.raft.dtos;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class LogEntry {
    private int index;
    private int term;
    private Command command;
}
