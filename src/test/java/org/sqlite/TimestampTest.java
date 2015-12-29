package org.sqlite;

import junit.framework.Assert;
import org.junit.Test;
import static org.junit.Assert.*;

import java.sql.*;
import java.io.File;

public class TimestampTest {

    @Test
    public void testTimestamp() throws Exception {
        Class.forName("org.sqlite.JDBC");
        Connection connection = DriverManager.getConnection("jdbc:sqlite:abc.db");
        try {
            Statement statement = connection.createStatement();
            statement.execute("drop table if exists foo");
            statement.execute("create table foo(bar)");
            statement.execute("insert into foo values ('2015-06-15 09:30:00');");
            ResultSet rs = statement.executeQuery("select bar from foo;");
            rs.next();
            Timestamp t = rs.getTimestamp("bar");
            Timestamp b2015 = Timestamp.valueOf("2015-01-01 13:03:01");
            assertTrue(t.toString(), t.after(b2015));
        } finally {
            connection.close();
        }
        new File("abc.db").delete();
    }

    @Test
    public void testDate() throws Exception {
        Class.forName("org.sqlite.JDBC");
        Connection connection = DriverManager.getConnection("jdbc:sqlite:xyz.db");
        try {
            Statement statement = connection.createStatement();
            statement.execute("drop table if exists foo2");
            statement.execute("create table foo2(bar)");
            statement.execute("insert into foo2 values ('2015-06-15');");
            ResultSet rs = statement.executeQuery("select bar from foo2;");
            rs.next();
            Date actual = rs.getDate("bar");
            Date wanted = Date.valueOf("2015-06-15");
            Assert.assertTrue(actual.toString(), actual.equals(wanted));
        } finally {
            connection.close();
        }
        new File("xyz.db").delete();
    }

    @Test
    public void testTime() throws Exception {
        Class.forName("org.sqlite.JDBC");
        Connection connection =
                DriverManager.getConnection("jdbc:sqlite:testTime.db");
        try {
            Statement statement = connection.createStatement();
            statement.execute("drop table if exists foo2");
            statement.execute("create table foo2(bar)");
            statement.execute("insert into foo2 values ('17:30:00');");
            ResultSet rs = statement.executeQuery("select bar from foo2;");
            rs.next();
            Time actual = rs.getTime("bar");
            Time wanted = Time.valueOf("17:30:00");
            Assert.assertTrue(actual.toString(), actual.equals(wanted));
        } finally {
            connection.close();
        }
        new File("testTime.db").delete();
    }


}
