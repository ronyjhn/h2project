package com.eu;

import com.google.common.base.Stopwatch;
import org.h2.jdbcx.JdbcConnectionPool;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by ronyjohn on 18/03/17.
 */
public class H2TestThreaded {
    private static final int RANDOM_NUMBER_FROM = 100;
    private static final int RANDOM_NUMBER_TO = 100000;
    private static final int TOTAL_ROWS = 300000;
    private static final long WARM_UP_DURATION_IN_MINS = 1;

    public static void main(final String[] args) throws Exception {
        final DataSource dataSource = initConnectionPool();

        creteTable(dataSource);
        insertIntoTable(dataSource);
        selectCount(dataSource);
        selectFirstPerson(dataSource);
        final Random random = new Random();
        final long start = System.currentTimeMillis();
        final long endAt = TimeUnit.MINUTES.toMillis(WARM_UP_DURATION_IN_MINS) + start;
        System.out.println("Warmup started at " + new java.util.Date(start) + ", will end at " + new java.util.Date(endAt));
        while (true) {
            selectQuery(dataSource, random.nextInt(RANDOM_NUMBER_TO - RANDOM_NUMBER_FROM) + RANDOM_NUMBER_FROM, false);
            if (endAt <= System.currentTimeMillis()) {
                System.out.println("Warmup completed");
                break;

            }
        }
        final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        for (int i = 0; i < 10; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    selectQuery(dataSource, random.nextInt(RANDOM_NUMBER_TO - RANDOM_NUMBER_FROM) + RANDOM_NUMBER_FROM, true);
                }
            });
        }
        executorService.shutdown();

    }


    public static void selectFirstPerson(final DataSource dataSource) {
        try (final Connection con = dataSource.getConnection(); final PreparedStatement pstmt = con.prepareStatement("Select name from PERSON LIMIT 1 OFFSET 0"); final ResultSet res = pstmt.executeQuery();) {
            if (res.next()) {
                System.out.println("Selected person NAME is " + res.getString(1));
            }
        } catch (SQLException sqe) {
            sqe.printStackTrace();
        }
    }

    public static void selectQuery(final DataSource dataSource, final int id, final boolean logIt) {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        int actualId = 0;
        try (final Connection con = dataSource.getConnection(); final PreparedStatement pstmt = con.prepareStatement("Select ID from PERSON where name=?");) {

            pstmt.setString(1, "name" + id);

            try (final ResultSet res = pstmt.executeQuery();) {


                if (res.next()) {
                    actualId = res.getInt(1);
                }
            }

        } catch (SQLException sqe) {
            sqe.printStackTrace();
        }
        stopwatch.stop();
        if (logIt) {
            System.out.println("Time taken by select query is " + stopwatch + " with id as " + id);
        }
    }


    public static void selectCount(final DataSource dataSource) {
        try (final Connection con = dataSource.getConnection(); final PreparedStatement pstmt = con.prepareStatement("Select count(*) from PERSON"); final ResultSet res = pstmt.executeQuery();) {

            if (res.next()) {
                System.out.println("Total person count is " + res.getInt(1));
            }

        } catch (SQLException sqe) {
            sqe.printStackTrace();
        }
    }

    public static void insertIntoTable(final DataSource dataSource) throws SQLException {
        final String insertSQL = "Insert into PERSON(ID,NAME,ADDRESS) values (?,?,?)";

        try (final Connection con = dataSource.getConnection(); final PreparedStatement pstmt = con.prepareStatement(insertSQL);) {
            con.setAutoCommit(false);
            for (int i = 0; i < TOTAL_ROWS; i++) {
                pstmt.setInt(1, i);
                pstmt.setString(2, "name" + i);
                pstmt.setString(3, "address" + i);
                pstmt.executeUpdate();
                pstmt.clearParameters();
            }

            con.commit();

        } catch (SQLException sqe) {
            sqe.printStackTrace();
        }
    }

    public static void creteTable(final DataSource dataSource) {
        final String createSql = "CREATE TABLE PERSON(id int primary key, name varchar(255),address varchar(255)); " +
                "CREATE INDEX IDX_1 on PERSON(name); ";
        try (final Connection con = dataSource.getConnection(); final PreparedStatement pstmt = con.prepareStatement(createSql);) {
            pstmt.executeUpdate();
        } catch (SQLException se) {
            se.printStackTrace();
        }

    }

    private static DataSource initConnectionPool() {
        final JdbcConnectionPool cp = JdbcConnectionPool.create(
                "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "", "");
        return cp;
    }

    private static Connection getDBConnection() throws SQLException {
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            return DriverManager.getConnection("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "",
                    "");

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
