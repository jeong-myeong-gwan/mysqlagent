package mj.mysqlagent.agent;

import mj.mysqlagent.collector.MysqlCollector;
import mj.mysqlagent.config.AgentConfig;
import mj.mysqlagent.config.ConfigLoader;
import mj.mysqlagent.fingerprint.SqlFingerprintEngine;
import mj.mysqlagent.model.AgentEvent;
import mj.mysqlagent.model.SlowLogEntry;
import mj.mysqlagent.queue.FileQueue;
import mj.mysqlagent.sampling.SlowQuerySampler;
import mj.mysqlagent.sender.GzipNdjsonSender;
import mj.mysqlagent.slowlog.SlowLogParser;
import mj.mysqlagent.slowlog.SlowLogTailer;
import mj.mysqlagent.util.SleepUtil;
import mj.oskspring.controller.HelloController;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AgentMain {

    public static void main(String[] args) throws Exception {
    	
    	log.debug("AgentMain started");
    			
    	
        Path configFile = args != null && args.length > 0
                ? Path.of(args[0])
                : Path.of("agent.yaml");

        AgentConfig cfg = ConfigLoader.load(configFile);

        long tenantId = cfg.tenantId;
        long instanceId = cfg.instanceId;
        String agentId = cfg.agentId;

        String jdbcUrl = cfg.mysql.url;
        String dbUser = cfg.mysql.user;
        String dbPass = cfg.mysql.password;

        String ingestUrl = cfg.ingest.url;
        String token = cfg.ingest.token;

        Path queueDir = Path.of(cfg.queue.path);
        Path slowLog = Path.of(cfg.slowlog.path);

        ObjectMapper om = new ObjectMapper();
        AtomicLong seq = new AtomicLong(0);

        MysqlCollector collector = new MysqlCollector(jdbcUrl, dbUser, dbPass);
        FileQueue queue = new FileQueue(queueDir, cfg.queue.maxSegmentBytes);
        GzipNdjsonSender sender = new GzipNdjsonSender(ingestUrl, token);

        SqlFingerprintEngine fingerprintEngine = new SqlFingerprintEngine();
        SlowQuerySampler sampler = new SlowQuerySampler(cfg.sampling);
        SlowLogParser slowLogParser = new SlowLogParser();

        ScheduledExecutorService ses = Executors.newScheduledThreadPool(2);

        ses.scheduleWithFixedDelay(() -> {
            try {
                long now = Instant.now().toEpochMilli();
                var payload = collector.collectStatus();

                AgentEvent ev = new AgentEvent(
                        tenantId,
                        instanceId,
                        agentId,
                        now,
                        seq.incrementAndGet(),
                        "metric",
                        payload
                );

                queue.appendLine(om.writeValueAsString(ev));
            } catch (Exception ignored) {
            }
        }, 0, 5, TimeUnit.SECONDS);

        ses.scheduleWithFixedDelay(() -> {
            try {
                long now = Instant.now().toEpochMilli();
                var digests = collector.collectTopDigests(50);

                AgentEvent ev = new AgentEvent(
                        tenantId,
                        instanceId,
                        agentId,
                        now,
                        seq.incrementAndGet(),
                        "digest",
                        Map.of("rows", digests)
                );

                queue.appendLine(om.writeValueAsString(ev));
            } catch (Exception ignored) {
            }
        }, 0, 20, TimeUnit.SECONDS);

        SlowLogTailer tailer = new SlowLogTailer(
                slowLog,
                cfg.slowlog.pollIntervalMillis,
                (lines) -> {
                    SlowLogEntry entry = slowLogParser.parse(lines);
                    if (entry == null || entry.sql() == null || entry.sql().isBlank()) {
                        return;
                    }

                    long now = Instant.now().toEpochMilli();
                    long queryTimeMillis = entry.queryTimeSec() == null
                            ? 0L
                            : Math.round(entry.queryTimeSec() * 1000.0);

                    String fingerprint = fingerprintEngine.fingerprint(entry.sql());

                    if (!sampler.shouldKeep(fingerprint, queryTimeMillis)) {
                        return;
                    }

                    Map<String, Object> payload = new LinkedHashMap<>();
                    payload.put("time", entry.time());
                    payload.put("userHost", entry.userHost());
                    payload.put("queryTimeSec", entry.queryTimeSec());
                    payload.put("queryTimeMillis", queryTimeMillis);
                    payload.put("lockTimeSec", entry.lockTimeSec());
                    payload.put("rowsSent", entry.rowsSent());
                    payload.put("rowsExamined", entry.rowsExamined());
                    payload.put("setTimestamp", entry.setTimestamp());
                    payload.put("sql", entry.sql());
                    payload.put("fingerprint", fingerprint);
                    payload.put("sampled", true);

                    AgentEvent ev = new AgentEvent(
                            tenantId,
                            instanceId,
                            agentId,
                            now,
                            seq.incrementAndGet(),
                            "slowlog",
                            payload
                    );

                    queue.appendLine(om.writeValueAsString(ev));
                }
        );

        Thread slowThread = new Thread(tailer, "slowlog-tailer");
        slowThread.setDaemon(true);
        slowThread.start();

        long senderBackoffMillis = 1000L;

        while (true) {
            try {
                var segOpt = queue.peekOldestSegment();

                if (segOpt.isEmpty()) {
                    queue.forceRoll();
                    SleepUtil.sleep(1000);
                    continue;
                }

                var seg = segOpt.get();
                boolean ok = sender.sendGzip(seg.bytes());

                if (ok) {
                    queue.ack(seg.path());
                    senderBackoffMillis = 1000L;
                } else {
                    SleepUtil.sleep(senderBackoffMillis);
                    senderBackoffMillis = Math.min(senderBackoffMillis * 2, 30000L);
                }

            } catch (Exception e) {
                SleepUtil.sleep(senderBackoffMillis);
                senderBackoffMillis = Math.min(senderBackoffMillis * 2, 30000L);
            }
        }
    }
}