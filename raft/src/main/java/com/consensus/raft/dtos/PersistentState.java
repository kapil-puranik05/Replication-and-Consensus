package com.consensus.raft.dtos;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class PersistentState {
    private int term;
    private Integer votedFor;
}
