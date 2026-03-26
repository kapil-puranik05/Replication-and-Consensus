package com.consensus.raft.infra;

import org.springframework.stereotype.Component;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Component
public class ElectionTimer {
    private final Random random;
    private final ScheduledExecutorService executorService;
    private ScheduledFuture<?> timeout;

    ElectionTimer() {
        executorService = Executors.newScheduledThreadPool(1);
        random = new Random();
    }

    public synchronized void startTimer(Runnable task) {
        if(timeout != null) {
            timeout.cancel(true);
        }
        System.out.println("Election Timer started");
        timeout = executorService.schedule(task, random.nextInt(10,25), TimeUnit.SECONDS);
    }

    public synchronized void cancelTimer() {
        if(timeout != null) {
            timeout.cancel(false);
            timeout = null;
        }
        System.out.println("Node became leader and timer is cancelled");
    }
}