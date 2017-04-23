package com.eu.grid;

import com.eu.models.Employee;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by ronyjohn on 09/04/17.
 */
public class EhcacheAppGetAll {
    public static void main(String[] args) {
        //Create a singleton CacheManager using defaults
        CacheManager manager = null;
        manager = CacheManager.create();

//Create a Cache specifying its configuration.
        final Cache cache = new Cache(
                new CacheConfiguration("employee_cache", 10000000)
                        .memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LFU).copyOnRead(true)
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
            final Set<Long> idSet = Sets.newHashSet();
            for (int i = 0; i < 5; i++) {
                long id = (long) random.nextInt(10000);
                while (idSet.contains(id)) {
                    id = (long) random.nextInt(10000);
                }

                idSet.add(id);
            }
            final List<Long> ids = Lists.newArrayList(idSet);
            search(manager, ids, false);
            if (end <= System.currentTimeMillis()) {
                break;
            }
        }

        final Scanner scanner = new Scanner(System.in);
        System.out.println("Please enter command");
        while (true) {
            final String command = scanner.nextLine();
            if ("q".equalsIgnoreCase(command)) {
                manager.shutdown();
                System.exit(1);
            }
            final int count = readCount(command, 50);
            final Set<Long> idSet = Sets.newHashSet();
            for (int i = 0; i < count; i++) {
                long id = (long) random.nextInt(10000);
                while (idSet.contains(id)) {
                    id = (long) random.nextInt(10000);
                }

                idSet.add(id);
            }
            final List<Long> ids = Lists.newArrayList(idSet);
            search(manager, ids, true);


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

    private static void search(final CacheManager cacheManager, final List<Long> ids, final boolean log) {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        final Cache cache = cacheManager.getCache("employee_cache");
        final Map<Object, Element> map = cache.getAll(ids);
        Preconditions.checkState(map.size() == ids.size(), "Violated result, map is %s, ids : %s", map, ids);
        stopwatch.stop();
        if (log) {
            System.out.println("Total time is " + stopwatch + ", size is " + ids.size());
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
