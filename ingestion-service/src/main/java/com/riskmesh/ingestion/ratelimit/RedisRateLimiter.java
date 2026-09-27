package com.riskmesh.ingestion.ratelimit;

import com.riskmesh.ingestion.config.RiskMeshSecurityProperties;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;


@Component
public class RedisRateLimiter {

    private static final Logger log = LoggerFactory.getLogger(RedisRateLimiter.class);
    @Value("${riskmesh.security.rate-limit.window-seconds}")
    private String WINDOW_SECONDS;

    private final StringRedisTemplate redisTemplate;
    private final RiskMeshSecurityProperties properties;
    private final DefaultRedisScript<List> script;

    public RedisRateLimiter(StringRedisTemplate redisTemplate, RiskMeshSecurityProperties properties) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.script = new DefaultRedisScript<>();
        this.script.setLocation(new ClassPathResource("/lua/rate_limit.lua"));
        this.script.setResultType(List.class);
    }

    public RateLimitResult checkMerchant(UUID merchantId) {
        return check("ratelimit:merchant:" + merchantId, properties.getRateLimit().getPerMerchantLimit());
    }

    public RateLimitResult checkGlobal() {
        return check("ratelimit:global", properties.getRateLimit().getGlobalLimit());
    }

    @SuppressWarnings("unchecked")
    private RateLimitResult check(String key, int limit) {
        try {
            List<Long> result = redisTemplate.execute(script, List.of(key), String.valueOf(limit), WINDOW_SECONDS);
            boolean allowed = result.get(0) == 1L;
            long retryAfter = result.get(1);
            return new RateLimitResult(allowed, retryAfter);
        } catch (RedisConnectionFailureException e) {
            log.warn("Redis unavailable for rate limiting on key {}; failing open", key, e);
            return new RateLimitResult(true,-1);
        }
    }
}
