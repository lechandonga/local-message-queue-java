package com.github.lechandonga.localmessagequeuejava;

import com.github.lechandonga.localmessagequeuejava.queue.LmqProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
@EnableConfigurationProperties(LmqProperties.class)
public class QueueConfig {
}
