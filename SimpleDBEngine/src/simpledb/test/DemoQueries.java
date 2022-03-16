package simpledb.test;

import simpledb.jdbc.embedded.EmbeddedDriver;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;

public class DemoQueries {
    private static void executeCmd(Statement stmt, String s) {
        System.out.println("\nSQL> " + s);
        SimpleIJ.execute(stmt, s);
    }

    private static void executeSingleTableQueries(Statement stmt) throws SQLException {
//        Show a few single table queries that involve distinct, order by, group by and
//        aggregates; some of these queries should result in using the indexes. Predicates should
//        include non-equal conditions.
        String[] arr = new String[]{
                "select sectionid, min(grade), max(grade), count(studentid) from enroll where sectionid > 100 and sectionid < 300 group by sectionid order by sectionid",
                "select distinct gradyear, sid from student where gradyear < 2019 order by gradyear asc, sid desc",
                "select count(sid), gradyear from student where gradyear <= 2018 group by gradyear",
                "select studentid from enroll where studentid = 20",
                "select distinct majorid from student where majorid = 90",
        };
        for (String s : arr) {
            executeCmd(stmt, "explain " + s);
            executeCmd(stmt, s);
        }
    }

    private static void executeTwoWayJoinQueries(Statement stmt) throws SQLException {
//        Show a few two-way join queries (i.e., queries involving two tables)
//        o   Do this for different join methods
        String[] arr = new String[]{
                "select sname, dname, majorid from student, dept where majorid=did",
                "select sname, dname, majorid from student, dept where majorid=did and majorid <=10",
                "select sid, eid from student, enroll where sid = studentid",
                "select sid, eid from student, enroll where sid = studentid and sid < 3",
        };
        for (String s : arr) {
            executeCmd(stmt, "explain " + s);
            executeCmd(stmt, s);
        }
    }

    private static void executeFourWayJoinQueries(Statement stmt) throws SQLException {
//        Show a few four-way join queries (i.e., queries involving four tables)
//        o   Do this for different join methods

        // query 0 and 3 failing
        // query 0 passes without order by
        // query 3 failing on null ptr, possible buffer overflow
        String[] arr = new String[]{
                "select sid, eid, deptid, cid from student, enroll, dept, course where sid = studentid and majorid = did and majorid = deptid order by sid",
                "select distinct sectid, cid, sid, deptid, gradyear, yearoffered from student, dept, course, section where courseid = cid and yearoffered <= gradyear and did = deptid and cid < 100",
                "select sectid, deptid, count(cid), count(sid) from student, dept, course, section where courseid = cid and yearoffered = gradyear and did = deptid and sid < 20 group by sectid",
        };
        for (String s : arr) {
            executeCmd(stmt, "explain " + s);
            executeCmd(stmt, s);
        }
    }

    public static void main(String[] args) {
        Driver d = new EmbeddedDriver();
        String url = "jdbc:simpledb:studentdb";

        try (Connection conn = d.connect(url, null); Statement stmt = conn.createStatement()) {
            executeSingleTableQueries(stmt);
            executeTwoWayJoinQueries(stmt);
            executeFourWayJoinQueries(stmt);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}