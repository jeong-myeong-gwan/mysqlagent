package mj.mysqlagent.sampling;

import mj.mysqlagent.config.AgentConfig;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class SlowQuerySampler {

    private final AgentConfig.Sampling cfg;
    private final Map<String, Long> topFingerprints;

    public SlowQuerySampler(AgentConfig.Sampling cfg) {
        this.cfg = cfg;
        this.topFingerprints = new LinkedHashMap<>(256, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
                return size() > cfg.keepTopFingerprintCount;
            }
        };
    }

    public synchronized boolean shouldKeep(String fingerprint, long queryTimeMillis) {
        if (!cfg.enabled) {
            return true;
        }

        if (queryTimeMillis >= cfg.verySlowThresholdMillis) {
            touch(fingerprint);
            return true;
        }

        if (queryTimeMillis >= cfg.slowThresholdMillis) {
            touch(fingerprint);
            return randomPass(0.5d);
        }

        if (topFingerprints.containsKey(fingerprint)) {
            touch(fingerprint);
            return randomPass(0.3d);
        }

        return randomPass(cfg.defaultRate);
    }

    private void touch(String fingerprint) {
        topFingerprints.put(fingerprint, System.currentTimeMillis());
    }

    private boolean randomPass(double rate) {
        return ThreadLocalRandom.current().nextDouble() < rate;
    }
}
