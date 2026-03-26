package com.consensus.raft.infra;

import org.springframework.stereotype.Component;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Component
public class LogReplicationTimer {
    private final ScheduledExecutorService executorService;
    private ScheduledFuture<?> heartbeatTask;

    LogReplicationTimer() {
        this.executorService = Executors.newScheduledThreadPool(1);
    }

    public synchronized void start(Runnable task) {
        cancel();
        heartbeatTask = executorService.scheduleAtFixedRate(task, 0, 1500, TimeUnit.MILLISECONDS);
    }

    public synchronized void cancel() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
            heartbeatTask = null;
        }
    }
}
