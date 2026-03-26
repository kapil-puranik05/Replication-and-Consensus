package com.consensus.raft.dtos;

import lombok.Data;

@Data
public class Command {
    public String key;
    public String value;
    public String type;
}
