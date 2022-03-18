package simpleclient.network;
import java.sql.*;

import simpledb.jdbc.network.NetworkDriver;

public class DemoQueries {
    private static void executeCmd(Statement stmt, String s) {
        System.out.println("\nSQL> " + s);
        SimpleIJ.execute(stmt, s);
    }

    private static void executeSingleTableQueries(Statement stmt) throws SQLException {
        String[] arr = new String[]{
                "select sectionid, min(grade), max(grade), count(studentid) from enroll where sectionid > 100 and sectionid < 300 group by sectionid order by sectionid",
                "select distinct gradyear, sid from student where gradyear < 2019 order by gradyear asc, sid desc",
                "select count(sid), gradyear from student where gradyear <= 2018 group by gradyear",
                "select studentid from enroll where studentid = 20",
                "select distinct majorid from student where majorid = 90",
                "select sname from student where majorid=20",
                "select sname from student where majorid<20",
                "select sname from student order by sname",
                "select sname, gradyear from student order by gradyear asc, sname desc",
                "select distinct gradyear from student",
                "select count(gradyear) from student",
                "select majorid, avg(gradyear) from student group by majorid",
                "select sum(sid) from student",
                "select min(did) from dept",
                "select max(did) from dept"
        };
        for (String s : arr) {
            executeCmd(stmt, "explain " + s);
            executeCmd(stmt, s);
        }
    }

    private static void executeTwoWayJoinQueries(Statement stmt) throws SQLException {
        String[] arr = new String[]{
                "select sname, dname, majorid from student, dept where majorid=did",
                "select sname, dname, majorid from student, dept where majorid=did and majorid <=10",
                "select sid, eid from student, enroll where sid = studentid",
                "select sid, eid from student, enroll where sid = studentid and sid < 3",
                "select sname, dname from student, dept where majorid = did and majorid <= 30 order by sname",
                "select sname, grade from student, enroll where sid = studentid and grade = 'A'",
                "select title, prof from course, section where cid = courseid and cid < 100 group by title, prof order by prof",
        };
        for (String s : arr) {
            executeCmd(stmt, "explain " + s);
            executeCmd(stmt, s);
        }
    }

    private static void executeFourWayJoinQueries(Statement stmt) throws SQLException {
        String[] arr = new String[]{
                "select sid, eid, deptid, cid from student, enroll, dept, course where sid = studentid and majorid = did and majorid = deptid and sid < 10",
                "select distinct sectid, cid, sid, deptid, gradyear, yearoffered from student, dept, course, section where courseid = cid and yearoffered <= gradyear and did = deptid and cid < 30",
                "select sname, gradyear, grade, title from student, enroll, section, course where sid = studentid and sectionid = sectid and courseid = cid and gradyear >= 2019 and sid < 15",
                "select distinct sname, dname from student, dept, course, section where majorid = did and majorid <= 30 and deptid = did and courseid = cid and cid < 40 and sectid < 30 and sid < 10"
        };
        for (String s : arr) {
            executeCmd(stmt, "explain " + s);
            executeCmd(stmt, s);
        }
    }

    public static void main(String[] args) {
        Driver d = new NetworkDriver();
        String url = "jdbc:simpledb://localhost";

        try (Connection conn = d.connect(url, null); Statement stmt = conn.createStatement()) {
            executeSingleTableQueries(stmt);
            executeTwoWayJoinQueries(stmt);
            executeFourWayJoinQueries(stmt);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}