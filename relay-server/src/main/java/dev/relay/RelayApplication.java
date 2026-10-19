package dev.relay;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;
import dev.relay.config.RelayProperties;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(RelayProperties.class)
public class RelayApplication {
  public static void main(String[] args) {
    SpringApplication.run(RelayApplication.class, args);
  }
}
