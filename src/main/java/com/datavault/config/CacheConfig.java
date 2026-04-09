package com.datavault.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

/**
 * Cache Configuration using Caffeine
 * In-memory cache - works on all operating systems
 */
@Configuration
@EnableCaching
public class CacheConfig {

    @Bean
    public CacheManager cacheManager() {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager(
            "databases",
            "database", 
            "tables",
            "table",
            "fields",
            "field",
            "glossaryTerms",
            "glossaryTerm",
            "changeHistory",
            "change",
            "tableQuality",
            "complianceStatus",
            "qualitySummary"
        );
        
        cacheManager.setCaffeine(Caffeine.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(60, TimeUnit.MINUTES)
            .recordStats()
        );
        
        return cacheManager;
    }
}
