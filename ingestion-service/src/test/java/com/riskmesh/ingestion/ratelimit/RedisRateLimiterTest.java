package com.riskmesh.ingestion.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import com.riskmesh.ingestion.config.RiskMeshSecurityProperties;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class RedisRateLimiterTest {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    private RedisRateLimiter rateLimiter;

    @BeforeEach
    void setUp() {
        RedisStandaloneConfiguration config =
                new RedisStandaloneConfiguration(redis.getHost(), redis.getMappedPort(6379));
        LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(config);
        connectionFactory.afterPropertiesSet();
        StringRedisTemplate redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.afterPropertiesSet();

        RiskMeshSecurityProperties properties = new RiskMeshSecurityProperties();
        properties.getRateLimit().setPerMerchantLimit(3);
        properties.getRateLimit().setGlobalLimit(1000);

        rateLimiter = new RedisRateLimiter(redisTemplate, properties , 60);
    }

    @Test
    void allowsRequestsWithinTheLimit() {
        UUID merchantId = UUID.randomUUID();

        RateLimitResult first = rateLimiter.checkMerchant(merchantId);
        RateLimitResult second = rateLimiter.checkMerchant(merchantId);
        RateLimitResult third = rateLimiter.checkMerchant(merchantId);

        assertThat(first.allowed()).isTrue();
        assertThat(second.allowed()).isTrue();
        assertThat(third.allowed()).isTrue();
    }

    @Test
    void rejectsRequestsOverTheLimitWithRetryAfter() {
        UUID merchantId = UUID.randomUUID();

        rateLimiter.checkMerchant(merchantId);
        rateLimiter.checkMerchant(merchantId);
        rateLimiter.checkMerchant(merchantId);
        RateLimitResult fourth = rateLimiter.checkMerchant(merchantId);

        assertThat(fourth.allowed()).isFalse();
        assertThat(fourth.retryAfterSeconds()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void eachMerchantHasAnIndependentBucket() {
        UUID merchantA = UUID.randomUUID();
        UUID merchantB = UUID.randomUUID();

        rateLimiter.checkMerchant(merchantA);
        rateLimiter.checkMerchant(merchantA);
        rateLimiter.checkMerchant(merchantA);
        RateLimitResult merchantAFourth = rateLimiter.checkMerchant(merchantA);
        RateLimitResult merchantBFirst = rateLimiter.checkMerchant(merchantB);

        assertThat(merchantAFourth.allowed()).isFalse();
        assertThat(merchantBFirst.allowed()).isTrue();
    }
}
