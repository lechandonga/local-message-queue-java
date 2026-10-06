package com.github.lechandonga.localmessagequeuejava.queue;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "lmq")
public class LmqProperties {

    private String defaultGroup = "default";
    private int maxRetries = 3;
    private Backoff backoff = new Backoff();
    private long leaseMs = 30000;
    private long sweepIntervalMs = 1000;

    public String getDefaultGroup() {
        return defaultGroup;
    }

    public void setDefaultGroup(String defaultGroup) {
        this.defaultGroup = defaultGroup;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public Backoff getBackoff() {
        return backoff;
    }

    public void setBackoff(Backoff backoff) {
        this.backoff = backoff;
    }

    public long getLeaseMs() {
        return leaseMs;
    }

    public void setLeaseMs(long leaseMs) {
        this.leaseMs = leaseMs;
    }

    public long getSweepIntervalMs() {
        return sweepIntervalMs;
    }

    public void setSweepIntervalMs(long sweepIntervalMs) {
        this.sweepIntervalMs = sweepIntervalMs;
    }

    public static class Backoff {
        private long baseMs = 1000;
        private double multiplier = 2.0;
        private long capMs = 30000;

        public long getBaseMs() {
            return baseMs;
        }

        public void setBaseMs(long baseMs) {
            this.baseMs = baseMs;
        }

        public double getMultiplier() {
            return multiplier;
        }

        public void setMultiplier(double multiplier) {
            this.multiplier = multiplier;
        }

        public long getCapMs() {
            return capMs;
        }

        public void setCapMs(long capMs) {
            this.capMs = capMs;
        }
    }
}
