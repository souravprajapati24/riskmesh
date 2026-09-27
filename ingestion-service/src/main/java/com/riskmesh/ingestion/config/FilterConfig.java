package com.riskmesh.ingestion.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.riskmesh.ingestion.ratelimit.IpBlocklistService;
import com.riskmesh.ingestion.ratelimit.RateLimitFilter;
import com.riskmesh.ingestion.ratelimit.RedisRateLimiter;
import com.riskmesh.ingestion.security.HmacSignatureFilter;
import com.riskmesh.ingestion.security.MerchantSecretResolver;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class FilterConfig {

    @Bean
    public HmacSignatureFilter hmacSignatureFilter(MerchantSecretResolver secretResolver, ObjectMapper objectMapper) {
        return new HmacSignatureFilter(secretResolver, objectMapper);
    }

    @Bean
    public RateLimitFilter rateLimitFilter(RedisRateLimiter rateLimiter, IpBlocklistService blocklistService,
                                           ObjectMapper objectMapper) {
        return new RateLimitFilter(rateLimiter, blocklistService, objectMapper);
    }

    @Bean
    public FilterRegistrationBean<HmacSignatureFilter> hmacFilterRegistration(HmacSignatureFilter filter) {
        FilterRegistrationBean<HmacSignatureFilter> registration = new FilterRegistrationBean<>(filter);
        registration.setOrder(1);
        registration.addUrlPatterns("/api/v1/transactions");
        return registration;
    }

    @Bean
    public FilterRegistrationBean<RateLimitFilter> rateLimitFilterRegistration(RateLimitFilter filter) {
        FilterRegistrationBean<RateLimitFilter> registration = new FilterRegistrationBean<>(filter);
        registration.setOrder(2);
        registration.addUrlPatterns("/api/v1/transactions");
        return registration;
    }
}
