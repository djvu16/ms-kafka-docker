package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import com.microservices.demo.config.TwitterToKafkaServiceDataConfig;
import com.microservices.demo.twitter.to.kafka.service.exception.TwitterToKafkaServiceException;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterToKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {

    private static final Logger logger = LoggerFactory.getLogger(MockKafkaStreamRunner.class);
    private static final Random RANDOM = new Random();
    private static final String[] WORDS = new String[]{
        "Lorem",
        "long",
        "established",
        "fact that",
        "a reader will",
        "be distracted by ",
        "the readable content",
        "of a page when ",
        "looking at its layout",
        "The point of using Lorem Ipsum is ",
        "that it has a more-or-less normal",
        "content here",
        "making it look like readable English",
        "Many desktop publishing ",
        "packages and web page ",
        "editors now use Lorem Ipsum ",
        "as their default model text",
    };

    private static final String tweetAsRawJson = "{" +
        "\"created_at\":\"{0}\"," +
        "\"id\":\"{1}\"," +
        "\"text\":\"{2}\"," +
        "\"user\":{\"id\":\"{3}\"}" +
        "}";

    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:MM:ss zz yyyy";
    private final TwitterToKafkaServiceDataConfig twitterToKafkaServiceDataConfig;
    private final TwitterToKafkaStatusListener twitterToKafkaStatusListener;

    public MockKafkaStreamRunner(TwitterToKafkaServiceDataConfig twitterToKafkaServiceDataConfig,
                                 TwitterToKafkaStatusListener twitterToKafkaStatusListener) {
        this.twitterToKafkaServiceDataConfig = twitterToKafkaServiceDataConfig;
        this.twitterToKafkaStatusListener = twitterToKafkaStatusListener;
    }

    private static String formatTweetAsJsonWithParams(String[] params) {
        String tweet = tweetAsRawJson;
        for (int i = 0; i < params.length; i++) {
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }

    private static String constructRandomTweet(String[] keywords, StringBuilder tweet, int tweetLength) {
        for (int i = 0; i < tweetLength; i++) {
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if (i == tweetLength / 2) {
                tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
            }
        }
        return tweet.toString().trim();
    }

    @Override
    public void start() throws TwitterException {
        String[] keywords = twitterToKafkaServiceDataConfig.getTwitterKeywords().toArray(new String[0]);
        int minTweetLength = twitterToKafkaServiceDataConfig.getMockMinTweetLength();
        int maxTweetLength = twitterToKafkaServiceDataConfig.getMockMaxTweetLength();
        long sleepTimeMs = twitterToKafkaServiceDataConfig.getMockSleepMs();
        logger.info("Starting mock filtering twitter streams for keywords {}", Arrays.toString(keywords));
        simulateTwitterStream(keywords, minTweetLength, maxTweetLength, sleepTimeMs);
    }

    private void simulateTwitterStream(
        String[] keywords, int minTweetLength, int maxTweetLength, long sleepTimeMs) throws TwitterException {
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                while (true) {
                    String formattedTweetAsRawJson = getFormatedTweet(keywords, minTweetLength, maxTweetLength);
                    Status status = TwitterObjectFactory.createStatus(formattedTweetAsRawJson);
                    twitterToKafkaStatusListener.onStatus(status);
                    sleep(sleepTimeMs);
                }
            } catch (TwitterException e) {
                logger.error("Error creating twitter status!", e);
                throw new RuntimeException(e);
            }
        });
    }

    private void sleep(long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            throw new TwitterToKafkaServiceException("Error while sleeping for waiting new status to create");
        }
    }

    private String getFormatedTweet(String[] keywords, int minTweetLength, int maxTweetLength) {
        String[] params = new String[]{
            ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
            String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
            getRandomTweetContent(keywords, minTweetLength, maxTweetLength),
            String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))
        };
        return formatTweetAsJsonWithParams(params);
    }

    private String getRandomTweetContent(String[] keywords, int minTweetLength, int maxTweetLength) {
        StringBuilder tweet = new StringBuilder();
        int tweetLength = RANDOM.nextInt(maxTweetLength - minTweetLength + 1) + minTweetLength;
        return constructRandomTweet(keywords, tweet, tweetLength);
    }
}
