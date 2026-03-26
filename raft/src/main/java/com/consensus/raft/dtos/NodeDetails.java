package com.consensus.raft.dtos;

import java.util.List;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class NodeDetails {
    private int nodeId;
    private List<Integer> peers;
}
