package com.eu;

import com.google.common.base.Stopwatch;

import javax.management.Query;
import java.sql.*;
import java.util.Random;

/**
 * Created by ronyjohn on 18/03/17.
 */
public class Main1 {
    public static void main(final String[] args) throws Exception {
        System.out.println("Hello World");
        final Connection con = getDBConnection();
        creteTable(con);
        insertIntoTable(con);
        selectCount(con);
        final Random random = new Random();
        for (int i = 0; i < 10; i++) {
            selectQuery(con, random.nextInt(10000 - 100) + 100);
        }
        selectFirstPerson(con);
        System.out.println(con);

    }

    public static void selectFirstPerson(final Connection con) {
        try (final PreparedStatement pstmt = con.prepareStatement("Select name from PERSON LIMIT 1 OFFSET 0"); final ResultSet res = pstmt.executeQuery();) {
            if (res.next()) {
                System.out.println("Selected person NAME is " + res.getString(1));
            }
        } catch (SQLException sqe) {
            sqe.printStackTrace();
        }
    }

    public static void selectQuery(final Connection con, final int id) {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        try (final PreparedStatement pstmt = con.prepareStatement("Select ID from PERSON where name=?");) {

            pstmt.setString(1, "name" + id);

            try (final ResultSet res = pstmt.executeQuery();) {


                if (res.next()) {
                    System.out.println("Selected person ID is " + res.getInt(1));
                }
            }

        } catch (SQLException sqe) {
            sqe.printStackTrace();
        }
        stopwatch.stop();
        System.out.println("Time taken by select query is " + stopwatch);
    }


    public static void selectCount(final Connection con) {
        try (final PreparedStatement pstmt = con.prepareStatement("Select count(*) from PERSON"); final ResultSet res = pstmt.executeQuery();) {

            if (res.next()) {
                System.out.println("Total person count is " + res.getInt(1));
            }

        } catch (SQLException sqe) {
            sqe.printStackTrace();
        }
    }

    public static void insertIntoTable(final Connection con) throws SQLException {
        final String insertSQL = "Insert into PERSON(ID,NAME,ADDRESS) values (?,?,?)";
        con.setAutoCommit(false);
        try (final PreparedStatement pstmt = con.prepareStatement(insertSQL);) {

            for (int i = 0; i < 30000; i++) {
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

    public static void creteTable(final Connection con) {
        final String createSql = "CREATE TABLE PERSON(id int primary key, name varchar(255),address varchar(255)); " +
                "CREATE INDEX IDX_1 on PERSON(name); ";
        try (final PreparedStatement pstmt = con.prepareStatement(createSql);) {
            pstmt.executeUpdate();
        } catch (SQLException se) {
            se.printStackTrace();
        }

    }

    private static Connection getDBConnection() throws SQLException {
        Connection dbConnection = null;
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
        }
        try {
            dbConnection = DriverManager.getConnection("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "",
                    "");
            return dbConnection;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return dbConnection;
    }
}
