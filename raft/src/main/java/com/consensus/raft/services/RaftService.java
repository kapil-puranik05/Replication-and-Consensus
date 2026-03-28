package com.consensus.raft.services;

import com.consensus.raft.dtos.Command;
import com.consensus.raft.dtos.LogEntry;
import com.consensus.raft.dtos.LogRequest;
import com.consensus.raft.dtos.LogResponse;
import com.consensus.raft.dtos.NodeDetails;
import com.consensus.raft.dtos.PersistentState;
import com.consensus.raft.dtos.VoteRequest;
import com.consensus.raft.dtos.VoteResponse;
import com.consensus.raft.infra.ElectionTimer;
import com.consensus.raft.infra.LogReplicationTimer;
import com.consensus.raft.infra.State;
import com.consensus.raft.util.Role;
import jakarta.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import tools.jackson.databind.ObjectMapper;

@Service
@RequiredArgsConstructor
public class RaftService {
    private static final String NODE_FILE = "node.json";
    private static final String STATE_FILE = "state.json";
    private static final String LOG_FILE = "raft.log";
    private final WebClient webClient;
    private final ExecutorService executorService;
    private final ElectionTimer electionTimer;
    private final LogReplicationTimer logReplicationTimer;
    private final ObjectMapper objectMapper;
    @Value("${raft.data-dir:.}")
    private String dataDir;
    private final Object lock = new Object();
    private final Map<String, String> stateMachine = new HashMap<>();
    private State state;

    @PostConstruct
    public void initialize() {
        state = new State();
        ensureDataDirectory();
        NodeDetails nodeDetails = loadNodeDetails();
        PersistentState persistentState = loadPersistentState();
        List<LogEntry> log = loadLog();
        state.setNodeId(nodeDetails.getNodeId());
        state.setPeers(nodeDetails.getPeers());
        state.setTerm(persistentState.getTerm());
        state.setVotedFor(persistentState.getVotedFor());
        state.setRole(Role.FOLLOWER);
        state.setLeaderId(null);
        state.setAckedLength(new HashMap<>());
        state.setSentLength(new HashMap<>());
        state.setVotesReceived(new HashSet<>());
        state.setLog(log);
        state.setCommitLength(0);
        rebuildStateMachine(log);
        state.setCommitLength(log.size());
        state.getAckedLength().put(state.getNodeId(), state.getLog().size());
        System.out.println(state);
    }

    public boolean onWriteRequest(Command command) {
        List<Integer> peers;
        synchronized (lock) {
            if (state.getRole() != Role.LEADER) {
                return false;
            }
            LogEntry logEntry = new LogEntry();
            logEntry.setIndex(state.getLog().size());
            logEntry.setTerm(state.getTerm());
            logEntry.setCommand(command);
            state.getLog().add(logEntry);
            appendLogEntry(logEntry);
            state.getAckedLength().put(state.getNodeId(), state.getLog().size());
            peers = new ArrayList<>(state.getPeers());
        }
        for (int peer : peers) {
            executorService.submit(() -> replicateLog(state.getNodeId(), peer));
        }
        return true;
    }

    public void onElectionTimeout() {
        List<Integer> peers;
        VoteRequest voteRequest;
        synchronized (lock) {
            if (state.getRole() == Role.LEADER) {
                return;
            }
            state.setTerm(state.getTerm() + 1);
            state.setRole(Role.CANDIDATE);
            state.setLeaderId(null);
            state.setVotedFor(state.getNodeId());
            state.getVotesReceived().clear();
            state.getVotesReceived().add(state.getNodeId());
            saveState();
            voteRequest = new VoteRequest();
            voteRequest.setCurrentTerm(state.getTerm());
            voteRequest.setNodeId(state.getNodeId());
            voteRequest.setLogLength(state.getLog().size());
            voteRequest.setLastTerm(lastLogTerm());
            peers = new ArrayList<>(state.getPeers());
            System.out.println("Node " + state.getNodeId() + " started election for term " + state.getTerm());
        }
        electionTimer.startTimer(this::onElectionTimeout);
        for (int peer : peers) {
            executorService.submit(() -> sendVoteRequest(peer, voteRequest));
        }
    }

    public VoteResponse onReceivingVoteRequest(VoteRequest voteRequest) {
        VoteResponse voteResponse = new VoteResponse();
        synchronized (lock) {
            if (voteRequest.getCurrentTerm() > state.getTerm()) {
                becomeFollower(voteRequest.getCurrentTerm(), null);
            }
            boolean logOk = isCandidateLogUpToDate(voteRequest);
            voteResponse.setVoterId(state.getNodeId());
            voteResponse.setTerm(state.getTerm());
            if (voteRequest.getCurrentTerm() == state.getTerm() && logOk && (state.getVotedFor() == null || state.getVotedFor().equals(voteRequest.getNodeId()))) {
                state.setVotedFor(voteRequest.getNodeId());
                saveState();
                voteResponse.setGranted(true);
                electionTimer.startTimer(this::onElectionTimeout);
            } else {
                voteResponse.setGranted(false);
            }
        }

        return voteResponse;
    }

    public void onReceivingVoteResponse(VoteResponse voteResponse) {
        synchronized (lock) {
            if (voteResponse.getTerm() > state.getTerm()) {
                becomeFollower(voteResponse.getTerm(), null);
                return;
            }
            if (state.getRole() != Role.CANDIDATE || voteResponse.getTerm() != state.getTerm() || !voteResponse.isGranted()) {
                return;
            }
            state.getVotesReceived().add(voteResponse.getVoterId());
            int clusterSize = state.getPeers().size() + 1;
            int quorum = (clusterSize / 2) + 1;
            if (state.getVotesReceived().size() >= quorum) {
                state.setRole(Role.LEADER);
                state.setLeaderId(state.getNodeId());
                initializeLeaderTracking();
                electionTimer.cancelTimer();
                logReplicationTimer.start(this::broadcastHeartbeats);
                System.out.println("Node " + state.getNodeId() + " became leader for term " + state.getTerm());
            }
        }
        broadcastHeartbeats();
    }

    public void replicateLog(int leaderId, int followerId) {
        LogRequest logRequest;
        synchronized (lock) {
            if (state.getRole() != Role.LEADER || state.getNodeId() != leaderId) {
                return;
            }
            int prefixLength = state.getSentLength().getOrDefault(followerId, 0);
            int prefixTerm = prefixLength == 0 ? 0 : state.getLog().get(prefixLength - 1).getTerm();
            List<LogEntry> suffix = new ArrayList<>();
            for (int index = prefixLength; index < state.getLog().size(); index++) {
                suffix.add(copyEntry(state.getLog().get(index)));
            }
            logRequest = new LogRequest();
            logRequest.setLeaderId(leaderId);
            logRequest.setTerm(state.getTerm());
            logRequest.setPrefixLength(prefixLength);
            logRequest.setPrefixTerm(prefixTerm);
            logRequest.setCommitLength(state.getCommitLength());
            logRequest.setSuffix(suffix);
        }
        try {
            LogResponse response = webClient.post()
                    .uri("http://localhost:" + followerId + "/appendEntries")
                    .bodyValue(logRequest)
                    .retrieve()
                    .bodyToMono(LogResponse.class)
                    .block();
            if (response != null) {
                onReceivingLogResponse(response);
            }
        } catch (Exception exception) {
            System.err.println("Failed to replicate log to peer " + followerId + ": " + exception.getMessage());
        }
    }

    public LogResponse onReceivingLogRequest(LogRequest logRequest) {
        LogResponse logResponse = new LogResponse();
        synchronized (lock) {
            if (logRequest.getTerm() > state.getTerm()) {
                becomeFollower(logRequest.getTerm(), logRequest.getLeaderId());
            }
            if (logRequest.getTerm() == state.getTerm()) {
                if (state.getRole() != Role.FOLLOWER) {
                    state.setRole(Role.FOLLOWER);
                    logReplicationTimer.cancel();
                }
                state.setLeaderId(logRequest.getLeaderId());
                electionTimer.startTimer(this::onElectionTimeout);
            }
            boolean logOk = state.getLog().size() >= logRequest.getPrefixLength() && (logRequest.getPrefixLength() == 0 || state.getLog().get(logRequest.getPrefixLength() - 1).getTerm() == logRequest.getPrefixTerm());
            logResponse.setNodeId(state.getNodeId());
            logResponse.setTerm(state.getTerm());
            if (logRequest.getTerm() == state.getTerm() && logOk) {
                appendEntries(logRequest.getPrefixLength(), logRequest.getCommitLength(), logRequest.getSuffix());
                logResponse.setAckedLength(logRequest.getPrefixLength() + logRequest.getSuffix().size());
                logResponse.setAcknowledged(true);
            } else {
                logResponse.setAckedLength(0);
                logResponse.setAcknowledged(false);
            }
        }
        return logResponse;
    }

    public void appendEntries(int prefixLength, int leaderCommit, List<LogEntry> suffix) {
        boolean logChanged = false;
        int suffixIndex = 0;
        while (suffixIndex < suffix.size() && prefixLength + suffixIndex < state.getLog().size()) {
            LogEntry localEntry = state.getLog().get(prefixLength + suffixIndex);
            LogEntry incomingEntry = suffix.get(suffixIndex);
            if (localEntry.getTerm() != incomingEntry.getTerm()) {
                while (state.getLog().size() > prefixLength + suffixIndex) {
                    state.getLog().remove(state.getLog().size() - 1);
                }
                logChanged = true;
                break;
            }
            suffixIndex++;
        }
        while (suffixIndex < suffix.size()) {
            LogEntry entry = copyEntry(suffix.get(suffixIndex));
            entry.setIndex(state.getLog().size());
            state.getLog().add(entry);
            suffixIndex++;
            logChanged = true;
        }
        if (logChanged) {
            rewriteLog();
        }
        int newCommitLength = Math.min(leaderCommit, state.getLog().size());
        if (newCommitLength > state.getCommitLength()) {
            applyEntries(state.getCommitLength(), newCommitLength);
            state.setCommitLength(newCommitLength);
        }
    }

    public void onReceivingLogResponse(LogResponse logResponse) {
        synchronized (lock) {
            if (logResponse.getTerm() > state.getTerm()) {
                becomeFollower(logResponse.getTerm(), null);
                return;
            }
            if (state.getRole() != Role.LEADER || logResponse.getTerm() != state.getTerm()) {
                return;
            }
            if (logResponse.isAcknowledged()) {
                int currentAck = state.getAckedLength().getOrDefault(logResponse.getNodeId(), 0);
                if (logResponse.getAckedLength() >= currentAck) {
                    state.getSentLength().put(logResponse.getNodeId(), logResponse.getAckedLength());
                    state.getAckedLength().put(logResponse.getNodeId(), logResponse.getAckedLength());
                    commitEntries();
                }
                return;
            }
            int nextPrefix = Math.max(0, state.getSentLength().getOrDefault(logResponse.getNodeId(), 0) - 1);
            state.getSentLength().put(logResponse.getNodeId(), nextPrefix);
            executorService.submit(() -> replicateLog(state.getNodeId(), logResponse.getNodeId()));
        }
    }

    public void commitEntries() {
        int quorum = (state.getPeers().size() + 1) / 2 + 1;
        int newCommitLength = state.getCommitLength();
        for (int candidateLength = state.getCommitLength() + 1; candidateLength <= state.getLog().size(); candidateLength++) {
            int acknowledgements = 1;
            for (int peer : state.getPeers()) {
                if (state.getAckedLength().getOrDefault(peer, 0) >= candidateLength) {
                    acknowledgements++;
                }
            }
            if (acknowledgements >= quorum && state.getLog().get(candidateLength - 1).getTerm() == state.getTerm()) {
                newCommitLength = candidateLength;
            }
        }
        if (newCommitLength > state.getCommitLength()) {
            applyEntries(state.getCommitLength(), newCommitLength);
            state.setCommitLength(newCommitLength);
        }
    }

    public Map<String, Object> getStatus() {
        synchronized (lock) {
            Map<String, Object> status = new LinkedHashMap<>();
            status.put("nodeId", state.getNodeId());
            status.put("role", state.getRole());
            status.put("term", state.getTerm());
            status.put("leaderId", state.getLeaderId());
            status.put("commitLength", state.getCommitLength());
            status.put("logLength", state.getLog().size());
            status.put("peers", new ArrayList<>(state.getPeers()));
            status.put("stateMachine", new LinkedHashMap<>(stateMachine));
            status.put("sentLength", new LinkedHashMap<>(state.getSentLength()));
            status.put("ackedLength", new LinkedHashMap<>(state.getAckedLength()));
            return status;
        }
    }

    public Integer getLeaderId() {
        synchronized (lock) {
            return state.getLeaderId();
        }
    }

    private void sendVoteRequest(int peer, VoteRequest voteRequest) {
        try {
            VoteResponse response = webClient.post()
                    .uri("http://localhost:" + peer + "/requestVote")
                    .bodyValue(voteRequest)
                    .retrieve()
                    .bodyToMono(VoteResponse.class)
                    .block();

            if (response != null) {
                onReceivingVoteResponse(response);
            }
        } catch (Exception exception) {
            System.err.println("Failed to contact peer " + peer + ": " + exception.getMessage());
        }
    }

    private void broadcastHeartbeats() {
        List<Integer> peers;
        synchronized (lock) {
            if (state.getRole() != Role.LEADER) {
                return;
            }
            peers = new ArrayList<>(state.getPeers());
        }
        for (int peer : peers) {
            executorService.submit(() -> replicateLog(state.getNodeId(), peer));
        }
    }

    private void initializeLeaderTracking() {
        int logSize = state.getLog().size();
        state.getVotesReceived().clear();
        state.getAckedLength().put(state.getNodeId(), logSize);
        for (int peer : state.getPeers()) {
            state.getSentLength().put(peer, logSize);
            state.getAckedLength().putIfAbsent(peer, 0);
        }
    }

    private void becomeFollower(int term, Integer leaderId) {
        state.setTerm(term);
        state.setRole(Role.FOLLOWER);
        state.setLeaderId(leaderId);
        state.setVotedFor(null);
        state.getVotesReceived().clear();
        saveState();
        logReplicationTimer.cancel();
        electionTimer.startTimer(this::onElectionTimeout);
    }

    private boolean isCandidateLogUpToDate(VoteRequest voteRequest) {
        int lastTerm = lastLogTerm();
        return voteRequest.getLastTerm() > lastTerm || (voteRequest.getLastTerm() == lastTerm && voteRequest.getLogLength() >= state.getLog().size());
    }

    private int lastLogTerm() {
        if (state.getLog().isEmpty()) {
            return 0;
        }
        return state.getLog().get(state.getLog().size() - 1).getTerm();
    }

    private void rebuildStateMachine(List<LogEntry> log) {
        stateMachine.clear();
        applyEntries(0, log.size(), log);
    }

    private void applyEntries(int fromInclusive, int toExclusive) {
        applyEntries(fromInclusive, toExclusive, state.getLog());
    }

    private void applyEntries(int fromInclusive, int toExclusive, List<LogEntry> sourceLog) {
        for (int index = fromInclusive; index < toExclusive; index++) {
            applyCommand(sourceLog.get(index).getCommand());
        }
    }

    private void applyCommand(Command command) {
        if (command == null || command.getKey() == null) {
            return;
        }
        String type = command.getType() == null ? "SET" : command.getType().trim().toUpperCase();
        if ("DELETE".equals(type)) {
            stateMachine.remove(command.getKey());
            return;
        }
        stateMachine.put(command.getKey(), command.getValue());
    }

    private void ensureDataDirectory() {
        try {
            Files.createDirectories(basePath());
        } catch (Exception exception) {
            throw new RuntimeException("Failed to create raft data directory", exception);
        }
    }

    private NodeDetails loadNodeDetails() {
        try {
            return objectMapper.readValue(resolvePath(NODE_FILE).toFile(), NodeDetails.class);
        } catch (Exception exception) {
            throw new RuntimeException("Failed to load node details", exception);
        }
    }

    private PersistentState loadPersistentState() {
        Path path = resolvePath(STATE_FILE);
        PersistentState persistentState = new PersistentState();
        if (Files.notExists(path)) {
            persistentState.setTerm(0);
            persistentState.setVotedFor(null);
            writePersistentState(persistentState);
            return persistentState;
        }
        try {
            return objectMapper.readValue(path.toFile(), PersistentState.class);
        } catch (Exception exception) {
            throw new RuntimeException("Failed to load persistent state", exception);
        }
    }

    private List<LogEntry> loadLog() {
        Path path = resolvePath(LOG_FILE);
        if (Files.notExists(path)) {
            return new ArrayList<>();
        }
        List<LogEntry> log = new ArrayList<>();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(path.toFile(), StandardCharsets.UTF_8))) {
            String line;
            int index = 0;
            while ((line = bufferedReader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                LogEntry logEntry = objectMapper.readValue(line, LogEntry.class);
                logEntry.setIndex(index++);
                log.add(logEntry);
            }
        } catch (Exception exception) {
            throw new RuntimeException("Failed to load raft log", exception);
        }
        return log;
    }

    private void saveState() {
        PersistentState persistentState = new PersistentState();
        persistentState.setTerm(state.getTerm());
        persistentState.setVotedFor(state.getVotedFor());
        writePersistentState(persistentState);
    }

    private void writePersistentState(PersistentState persistentState) {
        try (FileOutputStream fileOutputStream = new FileOutputStream(resolvePath(STATE_FILE).toFile())) {
            String json = objectMapper.writeValueAsString(persistentState);
            fileOutputStream.write(json.getBytes(StandardCharsets.UTF_8));
            fileOutputStream.getFD().sync();
        } catch (Exception exception) {
            throw new RuntimeException("Failed to write persistent state", exception);
        }
    }

    private void appendLogEntry(LogEntry logEntry) {
        try (BufferedWriter writer = Files.newBufferedWriter(resolvePath(LOG_FILE),StandardCharsets.UTF_8,StandardOpenOption.CREATE,StandardOpenOption.APPEND)) {
            writer.write(objectMapper.writeValueAsString(logEntry));
            writer.newLine();
            writer.flush();
        } catch (Exception exception) {
            throw new RuntimeException("Failed to append raft log", exception);
        }
    }

    private void rewriteLog() {
        try (BufferedWriter writer = Files.newBufferedWriter(resolvePath(LOG_FILE), StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            for (int index = 0; index < state.getLog().size(); index++) {
                LogEntry entry = state.getLog().get(index);
                entry.setIndex(index);
                writer.write(objectMapper.writeValueAsString(entry));
                writer.newLine();
            }
            writer.flush();
        } catch (Exception exception) {
            throw new RuntimeException("Failed to rewrite raft log", exception);
        }
    }

    private LogEntry copyEntry(LogEntry source) {
        LogEntry copy = new LogEntry();
        copy.setIndex(source.getIndex());
        copy.setTerm(source.getTerm());
        if (source.getCommand() != null) {
            Command command = new Command();
            command.setKey(source.getCommand().getKey());
            command.setValue(source.getCommand().getValue());
            command.setType(source.getCommand().getType());
            copy.setCommand(command);
        }
        return copy;
    }

    private Path basePath() {
        return Paths.get(dataDir).toAbsolutePath().normalize();
    }

    private Path resolvePath(String filename) {
        return basePath().resolve(filename);
    }
}
