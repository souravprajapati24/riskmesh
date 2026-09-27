package com.riskmesh.ingestion.ratelimit;

import com.riskmesh.ingestion.config.RiskMeshSecurityProperties;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class IpBlocklistService {

    private static final Logger log = LoggerFactory.getLogger(IpBlocklistService.class);
    private static final String BLOCKLIST_KEY = "blocklist:ip";
    private static final Duration VIOLATION_TTL = Duration.ofMinutes(10);
    private static final Duration BLOCK_DURATION = Duration.ofHours(1);

    private final StringRedisTemplate redisTemplate;
    private final RiskMeshSecurityProperties properties;

    public IpBlocklistService(StringRedisTemplate redisTemplate, RiskMeshSecurityProperties properties) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
    }

    public boolean isBlocked(String ip) {
        try {
            Double unblockAtEpochSeconds = redisTemplate.opsForZSet().score(BLOCKLIST_KEY, ip);
            if (unblockAtEpochSeconds == null) {
                return false;
            }
            return unblockAtEpochSeconds > Instant.now().getEpochSecond();
        } catch (RedisConnectionFailureException e) {
            log.warn("Redis unavailable for IP blocklist check on {}; failing open", ip, e);
            return false;
        }
    }

    public void recordViolation(String ip) {
        try {
            String key = "violations:ip:" + ip;
            Long count = redisTemplate.opsForValue().increment(key);
            redisTemplate.expire(key, VIOLATION_TTL);
            if (count != null && count > properties.getRateLimit().getIpViolationThreshold()) {
                long unblockAt = Instant.now().plus(BLOCK_DURATION).getEpochSecond();
                redisTemplate.opsForZSet().add(BLOCKLIST_KEY, ip, unblockAt);
            }
        } catch (RedisConnectionFailureException e) {
            log.warn("Redis unavailable for IP violation tracking on {}; skipping", ip, e);
        }
    }
}
