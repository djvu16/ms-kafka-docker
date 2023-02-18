package com.microservices.demo.twitter.to.kafka.service;

import com.microservices.demo.config.TwitterToKafkaServiceDataConfig;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.util.Arrays;

@SpringBootApplication
@ComponentScan(basePackages = "com.microservices.demo")
public class TwitterToKafkaServiceApplicaiton  implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaServiceApplicaiton.class);
    private TwitterToKafkaServiceDataConfig twitterToKafkaServiceDataConfig;

    private final StreamRunner streamRunner;

    public TwitterToKafkaServiceApplicaiton(TwitterToKafkaServiceDataConfig twitterToKafkaServiceDataConfig,
                                            StreamRunner streamRunner){
        this.twitterToKafkaServiceDataConfig = twitterToKafkaServiceDataConfig;
        this.streamRunner = streamRunner;
    }

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaServiceApplicaiton.class,args);
    }

    /**
     * Initialization ways
     * 1)@PostConstruct
     * 2)ApplicationListener
     * 3)CommandLineRunner
     * 3)@EventListener
     * @param args
     * @throws Exception
     */
    @Override
    public void run(String... args) throws Exception {
        LOG.info("App starts...");
        LOG.info(Arrays.toString(twitterToKafkaServiceDataConfig.getTwitterKeywords().toArray(new String[]{})));
        LOG.info(twitterToKafkaServiceDataConfig.getWelcomeMessage());
        streamRunner.start();
    }
}
