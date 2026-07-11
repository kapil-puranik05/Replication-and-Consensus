package com.consensus.raft.infra;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.consensus.raft.dtos.LogEntry;
import com.consensus.raft.util.Role;

import lombok.Data;

@Data
public class State {
    private String node;
    private List<String> peers;
    private int term;
    private String votedFor;
    private List<LogEntry> log;
    private String leader;
    private Role role;
    private int commitLength;
    private HashSet<String> votesReceived;
    private HashMap<String, Integer> sentLength; 
    private HashMap<String, Integer> ackedLength;
    private String gatewayAddress;

    @Override
    public String toString() {
        return "State{" +
            "node=" + node +
            ", role=" + role +
            ", term=" + term +
            ", votedFor=" + votedFor +
            ", leader=" + leader +
            ", commitLength=" + commitLength +
            ", logSize=" + (log == null ? 0 : log.size()) +
            ", votesReceived=" + (votesReceived == null ? 0 : votesReceived.size()) +
            ", sentLengthSize=" + (sentLength == null ? 0 : sentLength.size()) +
            ", ackedLengthSize=" + (ackedLength == null ? 0 : ackedLength.size()) +
            '}';
    }
}
