package com.consensus.raft.infra;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.consensus.raft.dtos.LogEntry;
import com.consensus.raft.util.Role;

import lombok.Data;

@Data
public class State {
    private int nodeId;
    private List<Integer> peers;
    private int term;
    private Integer votedFor;
    private List<LogEntry> log;
    private Integer leaderId;
    private Role role;
    private int commitLength;
    private HashSet<Integer> votesReceived;
    private HashMap<Integer, Integer> sentLength; 
    private HashMap<Integer, Integer> ackedLength;

    @Override
    public String toString() {
        return "State{" +
            "nodeId=" + nodeId +
            ", role=" + role +
            ", term=" + term +
            ", votedFor=" + votedFor +
            ", leaderId=" + leaderId +
            ", commitLength=" + commitLength +
            ", logSize=" + (log == null ? 0 : log.size()) +
            ", votesReceived=" + (votesReceived == null ? 0 : votesReceived.size()) +
            ", sentLengthSize=" + (sentLength == null ? 0 : sentLength.size()) +
            ", ackedLengthSize=" + (ackedLength == null ? 0 : ackedLength.size()) +
            '}';
    }
}
