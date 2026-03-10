package mj.mysqlagent.config;

public class AgentConfig {
    public long tenantId;
    public long instanceId;
    public String agentId;

    public Mysql mysql;
    public Slowlog slowlog;
    public Queue queue;
    public Ingest ingest;
    public Sampling sampling;

    public static class Mysql {
        public String url;
        public String user;
        public String password;
    }

    public static class Slowlog {
        public String path;
        public long pollIntervalMillis = 200;
    }

    public static class Queue {
        public String path;
        public long maxSegmentBytes = 2 * 1024 * 1024;
    }

    public static class Ingest {
        public String url;
        public String token;
    }

    public static class Sampling {
        public boolean enabled = true;
        public double defaultRate = 0.1;
        public long verySlowThresholdMillis = 3000;
        public long slowThresholdMillis = 1000;
        public int keepTopFingerprintCount = 100;
    }
}