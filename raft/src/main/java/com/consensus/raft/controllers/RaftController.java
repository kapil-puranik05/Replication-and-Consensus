package com.consensus.raft.controllers;

import com.consensus.raft.dtos.Command;
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
    public ResponseEntity<Map<String, Object>> submitCommand(@RequestBody Command command) {
        boolean accepted = raftService.onWriteRequest(command);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("accepted", accepted);
        response.put("leaderId", raftService.getLeader());
        if (accepted) {
            return ResponseEntity.accepted().body(response);
        }
        response.put("message", "This node is not the leader");
        return ResponseEntity.status(HttpStatus.CONFLICT).body(response);
    }

    @GetMapping("/state")
    public Map<String, Object> state() {
        return raftService.getStatus();
    }
}
