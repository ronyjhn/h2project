package com.eu.grid;

import com.eu.models.Employee;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by ronyjohn on 09/04/17.
 */
public class IgniteAppFST {
    public static void main(String[] args) {
        try (final Ignite ignite = Ignition.start(createConfiguration());) {
            createCacheRegion(ignite);
            insertIntoCache(ignite);
            final Random random = new Random();
            final long start = System.currentTimeMillis();
            final long end = start + TimeUnit.MINUTES.toMillis(1);
            System.out.println("Warm-up will end at " + new Date(end));
            while (true) {
                final List<Long> idSet = Lists.newArrayList();
                for (int i = 0; i < 5; i++) {
                    long id = (long) random.nextInt(10000);
                    while (idSet.contains(id)) {
                        id = (long) random.nextInt(10000);
                    }

                    idSet.add(id);
                }
                search(ignite, idSet, false);
                if (end <= System.currentTimeMillis()) {
                    break;
                }
            }

            final Scanner scanner = new Scanner(System.in);
            System.out.println("Please enter command");
            while (true) {
                final String command = scanner.nextLine();
                if ("q".equalsIgnoreCase(command)) {
                    System.exit(1);
                }
                final int count = readCount(command, 50);
                final List<Long> idSet = Lists.newArrayList();
                for (int i = 0; i < count; i++) {
                    long id = (long) random.nextInt(10000);
                    while (idSet.contains(id)) {
                        id = (long) random.nextInt(10000);
                    }

                    idSet.add(id);
                }
                search(ignite, idSet, true);


            }

        }
    }

    private static IgniteConfiguration createConfiguration() {
        final IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
        igniteConfiguration.setDiscoverySpi(new TcpDiscoverySpi());
        igniteConfiguration.setMarshaller(new FstAbstractNodeNameAwareMarshaller());
        return igniteConfiguration;
    }

    private static int readCount(final String command, final int defaultValue) {
        int result = defaultValue;
        try {
            result = Integer.parseInt(command);

        } catch (NumberFormatException nfe) {
            result = defaultValue;
        }
        return result;
    }

    private static void search(final Ignite ignite, final List<Long> ids, final boolean log) {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        final IgniteCache<Long, Employee> igniteCache = ignite.cache("EMPLOYEE");
        final Map<Long, Employee> map = igniteCache.getAll(Sets.newHashSet(ids));
        Preconditions.checkState(map.size() == ids.size(), "Violated result, map is %s, ids : %s", map, ids);
        stopwatch.stop();
        if (log) {
            System.out.println("Total time is " + stopwatch + ", size is " + ids.size());
        }

    }

    private static void insertIntoCache(final Ignite ignite) {
        final IgniteCache<Long, Employee> igniteCache = ignite.cache("EMPLOYEE");
        for (int i = 0; i < 100000; i++) {
            final Employee employee = new Employee();
            employee.setId((long) i);
            employee.setName("name" + i);
            employee.setAddress("address" + i);
            igniteCache.put((long) i, employee);
        }
    }

    private static void createCacheRegion(final Ignite ignite) {
        final CacheConfiguration<Long, Employee> cacheConfiguration = new CacheConfiguration<>("EMPLOYEE");
        cacheConfiguration.setIndexedTypes(Long.class, Employee.class);
        cacheConfiguration.setCacheMode(CacheMode.LOCAL);
        cacheConfiguration.setCopyOnRead(true);
        cacheConfiguration.setEvictionPolicy(new FifoEvictionPolicy(10000000));
        ignite.createCache(cacheConfiguration);

    }
}
