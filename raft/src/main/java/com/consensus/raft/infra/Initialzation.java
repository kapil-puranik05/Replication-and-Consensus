package com.consensus.raft.infra;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.consensus.raft.services.RaftService;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class Initialzation implements ApplicationRunner{
    private final ElectionTimer electionTimer;
    private final RaftService raftService;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Thread.sleep(3000);
        electionTimer.startTimer(raftService::onElectionTimeout);
    }
}
