package com.eu;

import com.google.common.base.Stopwatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by ronyjohn on 19/03/17.
 */
public class IgniteTest {
    private static final int RANDOM_NUMBER_FROM = 100;
    private static final int RANDOM_NUMBER_TO = 100000;
    private static final int TOTAL_ROWS = 300000;
    private static final long WARM_UP_DURATION_IN_MINS = 1;

    public static void main(String[] args) {
        try (final Ignite ignite = Ignition.start(createConfiguration());) {
            createCacheRegion(ignite);
            insertIntoCache(ignite);
            countRows(ignite);
            final Random random = new Random();
            final long start = System.currentTimeMillis();
            final long endAt = TimeUnit.MINUTES.toMillis(WARM_UP_DURATION_IN_MINS) + start;
            System.out.println("Warmup started at " + new Date(start) + ", will end at " + new Date(endAt));
            while (true) {
                selectQuery(ignite, (long) random.nextInt(RANDOM_NUMBER_TO - RANDOM_NUMBER_FROM) + RANDOM_NUMBER_FROM, false);
                if (endAt <= System.currentTimeMillis()) {
                    System.out.println("Warmup completed");
                    break;

                }
            }
            for (int i = 0; i < 10; i++) {
                selectQuery(ignite, (long) random.nextInt(RANDOM_NUMBER_TO - RANDOM_NUMBER_FROM) + RANDOM_NUMBER_FROM, true);
            }
        }
    }

    private static void countRows(final Ignite ignite) {
        final IgniteCache<Long, Person> igniteCache = ignite.cache("PERSON");
        final SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery("Select count(*) from PERSON");
        final QueryCursor<List<?>> queryCursor = igniteCache.query(sqlFieldsQuery);
        System.out.println("Total count is " + queryCursor.getAll());
    }

    private static void selectQuery(final Ignite ignite, final long id, final boolean logIt) {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        long actualId = 0;
        final IgniteCache<Long, Person> igniteCache = ignite.cache("PERSON");
        final SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery("Select ID from PERSON where name=?");
        sqlFieldsQuery.setArgs("name" + id);
        final QueryCursor<List<?>> queryCursor = igniteCache.query(sqlFieldsQuery);
        final List<?> list = queryCursor.getAll();
        if (!list.isEmpty()) {
            actualId = (long) ((List) list.get(0)).get(0);
        }
        stopwatch.stop();
        if (logIt) {
            System.out.println("Time taken by select query is " + stopwatch + " with id as " + id);
        }
    }

    private static IgniteConfiguration createConfiguration() {
        final IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
        igniteConfiguration.setDiscoverySpi(new TcpDiscoverySpi());

        return igniteConfiguration;
    }

    private static void insertIntoCache(final Ignite ignite) {
        final IgniteCache<Long, Person> igniteCache = ignite.cache("PERSON");
        for (int i = 0; i < TOTAL_ROWS; i++) {
            final Person person = new Person();
            person.setId((long) i);
            person.setName("name" + i);
            person.setAddress("address" + i);
            igniteCache.put((long) i, person);
        }
    }

    private static void createCacheRegion(final Ignite ignite) {
        final CacheConfiguration<Long, Person> cacheConfiguration = new CacheConfiguration<>("PERSON");
        cacheConfiguration.setIndexedTypes(Long.class, Person.class);
        cacheConfiguration.setCacheMode(CacheMode.LOCAL);
        cacheConfiguration.setEvictionPolicy(new FifoEvictionPolicy(10000000));
        ignite.createCache(cacheConfiguration);

    }


}
