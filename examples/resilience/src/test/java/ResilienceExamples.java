import io.github.resilience4j.bulkhead.*;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import io.vavr.control.Try;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.SlidingWindowType.COUNT_BASED;

public class ResilienceExamples {

    static MockWebServer server;
    static HttpUrl baseUrl;

    @BeforeAll
    static void setup() throws IOException {
        // Create a MockWebServer. These are lean enough that you can create a new
        // instance for every unit test.
        server = new MockWebServer();

        // Start the server.
        server.start();

        // Ask the server for its URL. You'll need this to make HTTP requests.
        baseUrl = server.url("/");
    }

    @AfterAll
    static void close() throws IOException {
        // Stop the server.
        server.close();
    }

    @Test
    void retryExample() {
        // Schedule some responses
        server.enqueue(new MockResponse().setResponseCode(500));
        server.enqueue(new MockResponse().setResponseCode(500));
        server.enqueue(new MockResponse().setResponseCode(200));

        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(baseUrl.url())
                .build();

        RetryConfig config = RetryConfig.custom()
                .failAfterMaxAttempts(true)
                .waitDuration(Duration.ofSeconds(1))
                .maxAttempts(3)
                .build();

        Retry retry = RetryRegistry.of(config).retry("downstream");
        retry.executeRunnable(() -> {
            var response = Try.of(() -> client.newCall(request).execute()).get();
            if (response.code() == 500) {
                System.out.println("Wrong");
                throw new RuntimeException();
            } else {
                System.out.println("Good");
            }
        });
    }

    @Test
    void timeoutExample() throws Exception {
        // Schedule some responses
        final Dispatcher dispatcher = new Dispatcher() {

            @Override
            public MockResponse dispatch (RecordedRequest request) throws InterruptedException {
                Thread.sleep(Duration.ofSeconds(1).toMillis());
                return new MockResponse().setResponseCode(200);
            }
        };
        server.setDispatcher(dispatcher);

        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(baseUrl.url())
                .build();


        TimeLimiterConfig config = TimeLimiterConfig
                .custom()
                .timeoutDuration(Duration.ofSeconds(2))
                .build();

        TimeLimiter timeLimiter = TimeLimiterRegistry
                .of(config)
                .timeLimiter("slow-response-timeouts");

        timeLimiter.executeFutureSupplier(
                () -> CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return client.newCall(request).execute();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                )
        );
    }


    Dispatcher simulator(int requestCount, int responseTime, int timeLimit, int recoveryTime) {
        return new Dispatcher() {
            AtomicInteger counter = new AtomicInteger(0);
            AtomicLong start = new AtomicLong(0);
            AtomicLong start_unavailable = new AtomicLong(0);
            AtomicBoolean available = new AtomicBoolean(true);

            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
                if (available.get()) {
                    if (counter.get() == 0) {
                        start.set(System.nanoTime());
                    } else if (counter.get() > requestCount) {
                        double elapsed = System.nanoTime() - start.get();
                        double elapsedSeconds = elapsed / 1_000_000_000;

                        if (elapsedSeconds < timeLimit) {
                            System.out.println("Exceeds number of requests");
                            counter.set(0);
                            available.set(false);
                            start_unavailable.set(System.nanoTime());
                            Thread.sleep(Duration.ofSeconds(responseTime).toMillis());
                            return new MockResponse().setResponseCode(500);
                        }
                        counter.set(0);
                        Thread.sleep(Duration.ofSeconds(responseTime).toMillis());
                        return new MockResponse().setResponseCode(200);
                    }

                    counter.incrementAndGet();
                    Thread.sleep(Duration.ofSeconds(responseTime).toMillis());
                    return new MockResponse().setResponseCode(200);
                } else {
                    double elapsed = System.nanoTime() - start_unavailable.get();
                    double elapsedSeconds = elapsed / 1_000_000_000;
                    if (elapsedSeconds > recoveryTime) {
                        available.set(true);
                    }
                    Thread.sleep(Duration.ofSeconds(responseTime).toMillis());
                    return new MockResponse().setResponseCode(500);
                }
            }
        };
    }

    @Test
    void rateLimiterExample() {
        server.setDispatcher(simulator(5, 1, 10, 5));

        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(baseUrl.url())
                .build();

        RateLimiterConfig config = RateLimiterConfig.custom()
                .limitForPeriod(1)
                .limitRefreshPeriod(Duration.ofSeconds(5))
                .timeoutDuration(Duration.ofSeconds(6))
                .build();

        RateLimiter rateLimiter = RateLimiterRegistry.of(config)
                .rateLimiter("peak-traffic-limiter");

        Runnable restrictedRunnable = RateLimiter.decorateRunnable(rateLimiter, () -> {
            try {
                var status = client.newCall(request).execute().code();
                if (status == 500) {
                    throw new RuntimeException("Failed");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        for (int i = 0; i < 20; i++) {
            try {
                restrictedRunnable.run();
                System.out.println("Good");
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }

    }

    @Test
    void circuitBreakerExample() {
        server.setDispatcher(simulator(5, 1, 10, 1));

        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(baseUrl.url())
                .build();

        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig
                .custom()
                .slidingWindowType(COUNT_BASED)
                .slidingWindowSize(6)
                .failureRateThreshold(50.0f)
                .waitDurationInOpenState(Duration.ofSeconds(2))
                .permittedNumberOfCallsInHalfOpenState(2)
                .build();

        CircuitBreaker circuitBreaker = CircuitBreaker.of(
                "too-many-failures-breaker",
                circuitBreakerConfig
        );
        circuitBreaker.transitionToClosedState();

        Runnable restrictedRunnable = circuitBreaker.decorateRunnable(() -> {
            try {
                var status = client.newCall(request).execute().code();
                if (status == 500) {
                    throw new RuntimeException("Failed");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        });

        for (int i = 0; i < 20; i++) {
            try {
                restrictedRunnable.run();
                System.out.println("Good");
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Test
    void bulkheadWithSemaphoreExample() {
        server.setDispatcher(simulator(5, 1, 5, 1));

        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(baseUrl.url())
                .build();

        BulkheadConfig config = BulkheadConfig.custom()
                .fairCallHandlingStrategyEnabled(true)
                .maxConcurrentCalls(5)
                .build();

        Bulkhead bulkhead = BulkheadRegistry.ofDefaults().bulkhead("semaphore-based", config);

        var restrictedRunnable = Bulkhead.decorateRunnable(bulkhead, () -> {
            try {
                var status = client.newCall(request).execute().code();
                if (status == 500) {
                    throw new RuntimeException("Failed");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        });

        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            var thread = new Thread(() -> {
                try {
                    restrictedRunnable.run();
                    System.out.println("Good");
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            });

            thread.start();
            threads.add(thread);
        }

        threads.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

    }

    @Test
    void bulkheadWithThreadPoolExample(){
        server.setDispatcher(simulator(5, 1, 5, 1));

        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(baseUrl.url())
                .build();

        ThreadPoolBulkheadConfig config = ThreadPoolBulkheadConfig.custom()
                .maxThreadPoolSize(5)
                .coreThreadPoolSize(5)
                .queueCapacity(5)
                .keepAliveDuration(Duration.ofSeconds(1))
                .build();

        ThreadPoolBulkhead bulkhead = ThreadPoolBulkheadRegistry
                .of(config)
                .bulkhead("thread-based");

        var restrictedRunnable = bulkhead.decorateRunnable(() -> {
            try {
                var status = client.newCall(request).execute().code();
                if (status == 500) {
                    throw new RuntimeException("Failed");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        });

        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            var thread = new Thread(() -> {
                try {
                    restrictedRunnable.get();
                    System.out.println("Good");
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            });

            thread.start();
            threads.add(thread);
        }

        threads.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

    }
}
