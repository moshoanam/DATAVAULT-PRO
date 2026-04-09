package com.datavault;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableCaching
@EnableJpaAuditing
@EnableAsync
@EnableScheduling
public class DataVaultApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataVaultApplication.class, args);
    }
}
