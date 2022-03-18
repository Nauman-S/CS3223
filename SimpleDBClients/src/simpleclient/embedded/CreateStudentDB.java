package simpleclient.embedded;
import java.sql.*;

import simpledb.jdbc.embedded.EmbeddedDriver;

public class CreateStudentDB {

    private static void createStudentTable(Statement stmt) throws SQLException {
        String s = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
        stmt.executeUpdate(s);
        System.out.println("Table STUDENT created.");
    }

    private static void createStudentIndex(Statement stmt) throws SQLException {
        String s = "create index majorIdIndex on student(MajorId) using hash";
        stmt.executeUpdate(s);
        System.out.println("majorIdIndex index created on student.");
        String s2 = "create index sidIndex on student(SId) using hash";
        stmt.executeUpdate(s2);
        System.out.println("sidIndex index created on student.");
    }

    private static void insertStudentTable(Statement stmt) throws SQLException {
        String s = "insert into STUDENT(SId, SName, GradYear, MajorId) values ";
        String[] studvals = {"(1, 'joe', 2021, 10)", "(2, 'amy', 2020, 20)", "(3, 'max', 2022, 10)"
                , "(4, 'sue', 2022, 20)", "(5, 'bob', 2020, 30)", "(6, 'kim', 2020, 20)", "(7, " +
                "'art', 2021, 20)", "(8, 'pat', 2019, 20)", "(9, 'lee', 2021, 10)", "(10, 'xenos', "
                + "2020, 540)", "(11, 'layne', 2017, 180)", "(12, 'kenn', 2021, 20)", "(13, 'winn',"
                + " 2022, 30)", "(14, 'glynn', 2019, 400)", "(15, 'aldon', 2016, 60)", "(16, " +
                "'maxie', 2022, 470)", "(17, 'georges', 2019, 550)", "(18, 'hewet', 2017, 490)",
                "(19, 'kory', 2017, 430)", "(20, 'catina', 2021, 620)", "(21, 'glenn', 2022, " +
                "550)", "(22, 'winne', 2022, 60)", "(23, 'craggie', 2017, 300)", "(24, 'theo', " +
                "2017, 110)", "(25, 'odo', 2021, 20)", "(26, 'jodi', 2022, 580)",
                "(27, 'isobel', 2018, 70)", "(28, 'gabe', 2017, 40)", "(29, 'kaylee', 2022, " +
                "560)", "(30, 'lillian', 2016, 400)", "(31, 'linette', 2019, 320)", "(32, " +
                "'lemar', 2018, 210)", "(33, 'cammy', 2016, 300)", "(34, 'gisela', 2022, 160)"
                , "(35, 'vic', 2018, 190)", "(36, 'georas', 2022, 510)", "(37, 'hewitt', 2017," +
                " 560)", "(38, 'jerrylee', 2018, 40)", "(39, 'leonanie', 2020, 40)", "(40, " +
                "'edsel', 2016, 390)", "(41, 'carlin', 2021, 370)", "(42, 'maude', 2018, 40)",
                "(43, 'eliot', 2018, 90)", "(44, 'carroll', 2016, 130)", "(45, 'kerstin', " +
                "2020, 450)", "(46, 'rollins', 2017, 550)", "(47, 'kati', 2020, 60)", "(48, " +
                "'esther', 2019, 410)", "(49, 'kristyn', 2022, 490)", "(50, 'andeee', 2021, 90)"};
        for (String studval : studvals) {
            stmt.executeUpdate(s + studval);
        }
        System.out.println("STUDENT records from " + 0 + " to " + (studvals.length - 1) + " " +
                "inserted.");
    }

    private static void createDeptTable(Statement stmt) throws SQLException {
        String s = "create table DEPT(DId int, DName varchar(8))";
        stmt.executeUpdate(s);
        System.out.println("Table DEPT created.");
    }

    private static void createDeptIndex(Statement stmt) throws SQLException {
        String s = "create index didIndex on dept(DId) using hash";
        stmt.executeUpdate(s);
        System.out.println("didIndex index created on dept.");
    }

    private static void insertDeptTable(Statement stmt) throws SQLException {
        String s = "insert into DEPT(DId, DName) values ";
        String[] deptvals = {"(10, 'compsci')", "(20, 'math')", "(30, 'drama')",
                "(40, 'accounting')", "(50, 'africanastudies')", "(60, 'anesthesiology')",
                "(70, 'anthropology')", "(80, 'architecture')", "(90, 'arthistory')", "(100, " +
                "'biologicalsciences')", "(110, 'biology')", "(120, 'biostatistics')", "(130, " +
                "'cardiology')", "(140, 'chemicalengineering')", "(150, 'chemistry')", "(160, " +
                "'dance')", "(170, 'dermatology')", "(180, 'economics')", "(190, " +
                "'electricalengineering')", "(200, 'endocrinology')", "(210, 'environmentalscience" +
                "')", "(220, 'epidemiology')", "(230, 'film')", "(240, 'financeandeconomics')"
                , "(250, 'french')", "(260, 'generalmedicine')", "(270, " +
                "'geneticsanddevelopment')", "(280, 'germaniclanguages')", "(290, 'globalsupport')"
                , "(300, 'hematology')", "(310, 'history')", "(320, 'infectiousdiseases')", "(330, " +
                "'italian')", "(340, 'management')", "(350, 'marketing')", "(360, " +
                "'mechanicalengineering')", "(370, 'medicine')", "(380, 'molecularmedicine')",
                "(390, 'music')", "(400, 'nephrology')", "(410, 'neurology')", "(420, " +
                "'neuroscience')", "(430, 'obstetrics&gynecology')", "(440, 'oncology')",
                "(450, 'ophthalmology')", "(460, 'orthopedicsurgery')", "(470, 'otolaryngology" +
                "')", "(480, 'pediatrics')", "(490, 'pharmacology')", "(500, 'philosophy')", "(510," +
                " 'physicaleducation')", "(520, 'physicsandastronomy')", "(530, 'physics')", "(540," +
                " 'politicalscience')", "(550, 'psychology')", "(560, 'radiationoncology')", "(570," +
                " 'radiology')", "(580, 'religion')", "(590, 'rheumatology')", "(600, 'slavic')",
                "(610, 'sociology')", "(620, 'sociomedicalsciences')", "(630, 'statistics')",
                "(640, 'surgery')", "(650, 'systemsbiology')", "(660, 'urology')", "(670, " +
                "'visualarts')", "(680, 'writing')"};
        for (String deptval : deptvals) {
            stmt.executeUpdate(s + deptval);
        }
        System.out.println("DEPT records from " + 0 + " to " + (deptvals.length - 1) + " " +
                "inserted.");

    }

    private static void createCourseTable(Statement stmt) throws SQLException {
        String s = "create table COURSE(CId int, Title varchar(20), DeptId int)";
        stmt.executeUpdate(s);
        System.out.println("Table COURSE created.");
    }

    private static void createCourseIndex(Statement stmt) throws SQLException {
        String s = "create index cidIndex on course(CId) using hash";
        stmt.executeUpdate(s);
        System.out.println("cidIndex index created on course.");
    }

    private static void insertCourseTable(Statement stmt) throws SQLException {
        String s = "insert into COURSE(CId, Title, DeptId) values ";
        String[] coursevals = {"(12, 'db systems', 10)", "(22, 'compilers', 10)",
                "(32, 'calculus', 20)", "(42, 'algebra', 20)", "(52, 'acting', 30)", "(62, " +
                "'elocution', 30)", "(72, 'electricity and magnetism', 280)", "(82, 'modern " +
                "methods in genetics', 340)", "(92, 'organic chemistry', 130)", "(102, 'sonic " +
                "narratives', 310)", "(112, 'data structures & algorithms', 200)", "(122, " +
                "'visual anthropology', 650)", "(132, 'law of evidence', 590)", "(142, " +
                "'dynamics of machines & vehicles', 610)", "(152, 'cell regulation and " +
                "cancer', 580)", "(162, 'data structures & algorithms', 180)", "(172, 'spanish ab " +
                "initio a', 200)", "(182, 'from words to interaction', 130)", "(192, 'health', 140)"
                , "(202, 'sex', 430)", "(212, 'economics of sports', 490)", "(222, 'video " +
                "journalism', 490)", "(232, 'drawing for design', 480)", "(242, 'epistemology" +
                "', 370)", "(252, 'revolution', 460)", "(262, 'software engineering', 680)", "(272," +
                " 'biological psychology', 60)", "(282, 'modern methods in genetics', 410)",
                "(292, 'politics and society', 650)", "(302, 'ice age earth', 670)", "(312, " +
                "'field biology and conservation skills', 320)", "(322, 'electricity and " +
                "electronics laboratory', 160)", "(332, 'business in action', 280)",
                "(342, 'low emission vehicle propulsion', 260)", "(352, 'forensic " +
                "linguistics', 240)", "(362, 'biological psychology', 280)", "(372, 'cell biology'," +
                " 440)", "(382, 'biological chemistry', 200)", "(392, 'design for " +
                "manufacture', 110)", "(402, 'low emission vehicle propulsion', 660)", "(412, " +
                "'forensic linguistics', 430)", "(422, 'southeast england field class', 90)",
                "(432, 'biological psychology', 140)", "(442, 'islam', 300)", "(452, " +
                "'fundamentals of machine learning', 290)", "(462, 'fundamentals of cancer " +
                "cell biology', 110)", "(472, 'critical perspectives on terrorism', 380)",
                "(482, 'electronic circuit & systems design', 240)", "(492, 'organic reaction "
                + "mechanisms', 670)", "(502, 'environmental economics', 170)", "(512, 'dynamics " +
                "of machines & vehicles', 480)", "(522, 'zombie media & arts practice', 460)",
                "(532, 'championing literacy placement', 180)", "(542, 'race and death in " +
                "global politics', 470)", "(552, 'fashion law', 600)", "(562, 'finite element " +
                "analysis', 500)", "(572, 'video journalism', 180)", "(582, 'crisis', 450)",
                "(592, 'introductory biology', 170)", "(602, 'revolution', 150)", "(612, 'data" +
                " coding & visualisation', 110)", "(622, 'geography overseas field class', " +
                "430)", "(632, 'discovering statistics', 300)", "(642, 'lyric poetry', 210)",
                "(652, 'video journalism', 40)", "(662, 'modern methods in genetics', 460)",
                "(672, 'urban futures', 570)", "(682, 'economics of sports', 330)", "(692, " +
                "'championing literacy placement', 540)", "(702, 'human rights', 470)"};
        for (String courseval : coursevals) {
            stmt.executeUpdate(s + courseval);
        }
        System.out.println("COURSE records from " + 0 + " to " + (coursevals.length - 1) + " " +
                "inserted.");
    }

    private static void createSectionTable(Statement stmt) throws SQLException {
        String s = "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
        stmt.executeUpdate(s);
        System.out.println("Table SECTION created.");
    }

    private static void createSectionIndex(Statement stmt) throws SQLException {
        String s = "create index sectidIndex on section(SectId) using hash";
        stmt.executeUpdate(s);
        System.out.println("sectidIndex index created on section.");
    }

    private static void insertSectionTable(Statement stmt) throws SQLException {
        String s = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values ";
        String[] sectvals = {"(13, 12, 'turing', 2018)", "(23, 12, 'turing', 2019)", "(33, 32, " +
                "'newton', 2019)", "(43, 32, 'einstein', 2017)", "(53, 62, 'brando', 2018)", "(63, "
                + "92, 'baggaley', 2019)", "(73, 442, 'concklin', 2015)",
                "(83, 92, 'pomphrey', 2021)", "(93, 112, 'barnby', 2012)", "(103, 382, " +
                "'byass', 2014)", "(113, 672, 'ardling', 2019)", "(123, 642, 'halsey', 2015)",
                "(133, 102, 'edmenson', 2019)", "(143, 332, 'blonfield', 2011)", "(153, 192, " +
                "'scryne', 2015)", "(163, 652, 'jurek', 2018)", "(173, 382, 'weldrick', 2014)"
                , "(183, 692, 'aviss', 2012)", "(193, 572, 'dickens', 2010)", "(203, 442, " +
                "'skittle', 2014)", "(213, 62, 'coucher', 2016)", "(223, 142, 'mcgirr', 2020)"
                , "(233, 12, 'badrock', 2015)", "(243, 502, 'thorns', 2021)", "(253, 432, 'kless', " +
                "2014)", "(263, 52, 'mcilhone', 2015)", "(273, 182, 'bediss', 2018)", "(283, " +
                "272, 'huskinson', 2018)", "(293, 242, 'danson', 2010)", "(303, 472, " +
                "'crollman', 2022)", "(313, 562, 'clements', 2010)", "(323, 702, 'duncklee', " +
                "2010)", "(333, 322, 'claridge', 2021)", "(343, 222, 'crumby', 2015)", "(353, " +
                "82, 'managh', 2017)", "(363, 42, 'goldstein', 2017)", "(373, 252, 'eaves', 2018)",
                "(383, 702, 'filgate', 2013)", "(393, 432, 'darycott', 2017)", "(403, 532, " +
                "'macpaden', 2014)", "(413, 242, 'clamp', 2012)", "(423, 92, 'pigeon', 2010)",
                "(433, 232, 'idwal evans', 2012)", "(443, 402, 'nussey', 2015)", "(453, 122, " +
                "'verissimo', 2019)", "(463, 222, 'fairebrother', 2018)", "(473, 132, " +
                "'macane', 2018)", "(483, 192, 'kundert', 2019)", "(493, 382, 'bampkin', 2012)",
                "(503, 672, 'frangello', 2012)", "(513, 682, 'daice', 2022)", "(523, 422, " +
                "'woltman', 2014)", "(533, 622, 'stormont', 2012)", "(543, 502, 'feria', 2012)"
                , "(553, 522, 'jumeau', 2018)", "(563, 142, 'grint', 2017)", "(573, 702, " +
                "'andrich', 2013)", "(583, 702, 'reaman', 2021)", "(593, 422, 'grube', 2018)",
                "(603, 22, 'mathiot', 2010)", "(613, 212, 'smitherman', 2010)", "(623, 472, " +
                "'blant', 2019)", "(633, 422, 'gino', 2017)", "(643, 472, 'farrance', 2012)",
                "(653, 52, 'pinare', 2015)", "(663, 182, 'petrou', 2018)", "(673, 242, 'kurtis', " +
                "2022)", "(683, 402, 'dodge', 2010)", "(693, 42, 'goadbie', 2013)", "(703, " +
                "292, 'keer', 2011)", "(713, 562, 'tropman', 2018)", "(723, 632, 'ambler', 2018)",
                "(733, 92, 'menier', 2014)", "(743, 472, 'cheal', 2013)", "(753, 682, 'dugan'," +
                " 2019)", "(763, 132, 'merwe', 2018)", "(773, 382, 'ducket', 2011)", "(783, " +
                "82, 'heeks', 2020)", "(793, 582, 'sampey', 2019)", "(803, 92, 'cowtherd', 2021)"};
        for (String sectval : sectvals) {
            stmt.executeUpdate(s + sectval);
        }
        System.out.println("SECTION records from " + 0 + " to " + (sectvals.length - 1) + " " +
                "inserted.");
    }

    private static void createEnrollTable(Statement stmt) throws SQLException {
        String s = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))";
        stmt.executeUpdate(s);
        System.out.println("Table ENROLL created.");
    }

    private static void createEnrollIndex(Statement stmt) throws SQLException {
        String s = "create index studentIdIndex on enroll(StudentId) using Btree";
        stmt.executeUpdate(s);
        System.out.println("studentIdIndex index created on enroll.");
    }

    private static void insertEnrollTable(Statement stmt) throws SQLException {
        String s = "insert into ENROLL(EId, StudentId, SectionId, Grade) values ";
        String[] enrollvals = {"(14, 1, 13, 'A')", "(24, 1, 43, 'C')", "(34, 2, 43, 'B+')",
                "(44, 4, 33, 'B')", "(54, 4, 53, 'A')", "(64, 6, 53, 'A')", "(74, 39, 83, 'B')"
                , "(84, 22, 763, 'B-')", "(94, 16, 43, 'A+')", "(104, 36, 273, 'A')", "(114, " +
                "5, 753, 'B+')", "(124, 27, 103, 'D+')", "(134, 8, 603, 'A+')", "(144, 26, " +
                "293, 'A+')", "(154, 49, 693, 'B-')", "(164, 28, 133, 'B-')", "(174, 36, 603, 'D+')"
                , "(184, 39, 433, 'A-')", "(194, 42, 783, 'C')", "(204, 27, 463, 'A-')",
                "(214, 29, 743, 'C+')", "(224, 43, 93, 'B')", "(234, 19, 323, 'F')", "(244, " +
                "19, 353, 'A-')", "(254, 29, 803, 'B+')", "(264, 31, 133, 'A')", "(274, 18, 83, " +
                "'B+')", "(284, 20, 323, 'A+')", "(294, 17, 463, 'A')", "(304, 30, 763, 'A+')"
                , "(314, 28, 283, 'D+')", "(324, 1, 753, 'B')", "(334, 7, 653, 'C+')", "(344, " +
                "49, 73, 'D')", "(354, 1, 373, 'C+')", "(364, 18, 383, 'B+')", "(374, 15, 313, 'A')"
                , "(384, 45, 483, 'A+')", "(394, 27, 203, 'C')", "(404, 1, 13, 'C')", "(414, " +
                "29, 263, 'A-')", "(424, 49, 543, 'D')", "(434, 39, 423, 'C+')", "(444, 42, " +
                "593, 'F')", "(454, 40, 323, 'B+')", "(464, 24, 333, 'B+')", "(474, 21, 323, 'C')",
                "(484, 37, 643, 'C')", "(494, 2, 153, 'F')", "(504, 5, 673, 'F')", "(514, 3, " +
                "203, 'D')", "(524, 34, 153, 'B-')", "(534, 48, 73, 'C')", "(544, 33, 233, 'A')",
                "(554, 20, 503, 'B+')", "(564, 36, 203, 'A+')", "(574, 17, 603, 'F')", "(584, " +
                "25, 293, 'A')", "(594, 50, 423, 'A-')", "(604, 43, 63, 'C')", "(614, 27, 783," +
                " 'D+')", "(624, 3, 763, 'D+')", "(634, 4, 263, 'C')", "(644, 6, 553, 'F')", "(654," +
                " 28, 393, 'A')", "(664, 36, 683, 'A-')", "(674, 4, 683, 'C+')", "(684, 33, " +
                "163, 'A+')", "(694, 8, 493, 'F')", "(704, 45, 563, 'B-')", "(714, 4, 153, " +
                "'A')", "(724, 29, 283, 'A+')", "(734, 40, 753, 'D')", "(744, 4, 773, 'B+')",
                "(754, 19, 753, 'C')", "(764, 9, 423, 'A+')", "(774, 48, 513, 'A-')", "(784, " +
                "8, 13, 'A-')", "(794, 44, 453, 'D+')", "(804, 20, 43, 'B')", "(814, 7, 153, " +
                "'D')", "(824, 5, 373, 'A')", "(834, 50, 553, 'F')", "(844, 35, 623, 'B-')", "(854," +
                " 18, 323, 'C')", "(864, 44, 733, 'B+')", "(874, 10, 693, 'A')",
                "(884, 15, 553, 'A+')", "(894, 32, 663, 'A-')", "(904, 47, 263, 'A')", "(914, " +
                "49, 13, 'C+')", "(924, 3, 513, 'B')", "(934, 32, 603, 'D')", "(944, 1, 293, " +
                "'C+')", "(954, 43, 43, 'A-')", "(964, 36, 473, 'D+')", "(974, 15, 653, 'A')",
                "(984, 23, 223, 'C+')", "(994, 24, 133, 'D')", "(1004, 35, 53, 'B')"};
        for (String enrollval : enrollvals) {
            stmt.executeUpdate(s + enrollval);
        }
        System.out.println("ENROLL records from " + 0 + " to " + (enrollvals.length - 1) + " " +
                "inserted.");
    }

    private static void createStudentDB(Driver d, String url) {
        try (Connection conn = d.connect(url, null); Statement stmt = conn.createStatement()) {
            createStudentTable(stmt);
            createStudentIndex(stmt);
            createDeptTable(stmt);
            createDeptIndex(stmt);
            createCourseTable(stmt);
            createCourseIndex(stmt);
            createSectionTable(stmt);
            createSectionIndex(stmt);
            createEnrollTable(stmt);
            createEnrollIndex(stmt);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try (Connection conn = d.connect(url, null); Statement stmt = conn.createStatement()) {
            insertStudentTable(stmt);
            insertEnrollTable(stmt);
            insertDeptTable(stmt);
            insertCourseTable(stmt);
            insertSectionTable(stmt);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Driver d = new EmbeddedDriver();
        String url = "jdbc:simpledb:studentdb";

        createStudentDB(d, url);
    }
}
