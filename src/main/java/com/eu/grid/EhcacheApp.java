package com.eu.grid;

import com.eu.models.Employee;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

import java.util.Date;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;



/**
 * Created by ronyjohn on 09/04/17.
 */
public class EhcacheApp {
    public static void main(String[] args) {
        //Create a singleton CacheManager using defaults
        CacheManager manager = CacheManager.create();

//Create a Cache specifying its configuration.
        final Cache cache = new Cache(
                new CacheConfiguration("employee_cache", 10000000)
                        .memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LFU)
                        .eternal(true)
        );
        manager.addCache(cache);
        loadCache(cache);
        System.out.println("Total cache size : " + cache.getSize());
        final Random random = new Random();
        final long start = System.currentTimeMillis();
        final long end = start + TimeUnit.MINUTES.toMillis(1);
        System.out.println("Warm-up will end at " + new Date(end));
        while (true) {
            final long id = (long) random.nextInt(10000);
            search(cache, id, false);
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
            final Stopwatch stopwatch = Stopwatch.createStarted();
            for (int i = 0; i < count; i++) {
                final long id = (long) random.nextInt(10000);
                search(cache, id, true);

            }
            stopwatch.stop();
            System.out.println("Total time " + stopwatch);

        }


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

    private static void search(final Cache cache, final long id, final boolean log) {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        final Element element = cache.get(id);
        Preconditions.checkState(((Employee) element.getObjectValue()).getId() == id);
        stopwatch.stop();
        if (log) {
            System.out.println("Total time is " + stopwatch);
        }

    }

    private static void loadCache(final Cache cache) {
        for (int i = 0; i < 100000; i++) {
            final Employee employee = new Employee();
            employee.setId((long) i);
            employee.setName("name" + i);
            employee.setAddress("address" + i);
            cache.put(new Element((long) i, employee));
        }
    }
}
