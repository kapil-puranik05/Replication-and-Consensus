package com.consensus.raft.controllers;

import com.consensus.raft.dtos.Command;
import com.consensus.raft.dtos.CommandResponse;
import com.consensus.raft.dtos.LogRequest;
import com.consensus.raft.dtos.LogResponse;
import com.consensus.raft.dtos.VoteRequest;
import com.consensus.raft.dtos.VoteResponse;
import com.consensus.raft.services.RaftService;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class RaftController {
    private final RaftService raftService;

    @PostMapping("/requestVote")
    public VoteResponse requestVote(@RequestBody VoteRequest voteRequest) {
        return raftService.onReceivingVoteRequest(voteRequest);
    }

    @PostMapping("/appendEntries")
    public LogResponse appendEntries(@RequestBody LogRequest logRequest) {
        return raftService.onReceivingLogRequest(logRequest);
    }

    @PostMapping("/command")
    public ResponseEntity<?> submitCommand(@RequestBody Command command) {
        boolean accepted = raftService.onWriteRequest(command);
        String leader = raftService.getLeader();
        CommandResponse response = new CommandResponse();
        response.setAccepted(accepted);
        response.setLeaderAddress(leader);
        if(accepted) {
            response.setMessage("Accepted");
            return new ResponseEntity<>(response, HttpStatus.ACCEPTED);
        }
        response.setMessage("This node is not the leader");
        return new ResponseEntity<>(response, HttpStatus.CONFLICT);
    }

    @GetMapping("/{key}")
    public ResponseEntity<?> getValue(@PathVariable String key) {
        String val = raftService.getValue(key);
        if(val != null && !val.isBlank()) {
            return new ResponseEntity<>(val, HttpStatus.OK);
        } else {
            return new ResponseEntity<>("Value for given key was not found", HttpStatus.NOT_FOUND);
        }
    }

    @GetMapping("/state")
    public Map<String, Object> state() {
        return raftService.getStatus();
    }
}
