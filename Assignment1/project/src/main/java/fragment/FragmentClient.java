package fragment;

import java.sql.*;
import java.util.*;

public class FragmentClient {
    private static final String USER = "";
    private static final String PASSWORD = "";
    private Map<Integer, Connection> connectionPool;
    private Router router;
    private int numFragments;

    public FragmentClient(int numFragments) {

        this.numFragments = numFragments;
        this.router = new Router(numFragments);
        this.connectionPool = new HashMap<>();

    }

    /**
     * Initialize JDBC connections to all N fragments.
     */
    public void setupConnections() {
        try {
            for (int i = 0; i < numFragments; i++) {
                String url = "jdbc:postgresql://localhost:5432/baseline";
                Connection conn = DriverManager.getConnection(url, USER, PASSWORD);
                connectionPool.put(i, conn);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Insert a student into the correct fragment.
     */
    public void insertStudent(String studentId, String name, int age, String email) {
        try {
            int fid = router.getFragmentId(studentId);
            Connection conn = connectionPool.get(fid);

            PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO Student (student_id, name, age, email) VALUES (?, ?, ?, ?)");
            ps.setString(1, studentId);
            ps.setString(2, name);
            ps.setInt(3, age);
            ps.setString(4, email);
            ps.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * TODO: Route the grade to the correct shard and execute the INSERT.
     */
    public void insertGrade(String studentId, String courseId, int score) {
        try {
            int fid = router.getFragmentId(studentId);
            Connection conn = connectionPool.get(fid);

            PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO Grade (student_id, course_id, score) " +
                            "VALUES (?, ?, ?)");

            ps.setString(1, studentId);
            ps.setString(2, courseId);
            ps.setInt(3, score);

            ps.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void updateGrade(String studentId,
            String courseId, int newScore) {
        try {
            int fid = router.getFragmentId(studentId);
            Connection conn = connectionPool.get(fid);

            PreparedStatement ps = conn.prepareStatement(
                    "UPDATE Grade SET score = ? " +
                            "WHERE student_id = ? AND course_id = ?");

            ps.setInt(1, newScore);
            ps.setString(2, studentId);
            ps.setString(3, courseId);

            ps.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteStudentFromCourse(String studentId,
            String courseId) {
        try {
            int fid = router.getFragmentId(studentId);
            Connection conn = connectionPool.get(fid);

            PreparedStatement ps = conn.prepareStatement(
                    "DELETE FROM Grade " +
                            "WHERE student_id = ? AND course_id = ?");

            ps.setString(1, studentId);
            ps.setString(2, courseId);

            ps.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * TODO: Fetch the student's name and email.
     */
    public String getStudentProfile(String studentId) {
        try {
            int fid = router.getFragmentId(studentId);
            Connection conn = connectionPool.get(fid);

            PreparedStatement ps = conn.prepareStatement(
                    "SELECT name, email FROM Student " +
                            "WHERE student_id = ?");

            ps.setString(1, studentId);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                return rs.getString("name") + "," +
                        rs.getString("email");
            }

            return "";

        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    /**
     * TODO: Calculate the average score per department.
     */
    public String getAvgScoreByDept() {
        try {
            Random rand = new Random();
            int fid = rand.nextInt(numFragments);

            Connection conn = connectionPool.get(fid);

            PreparedStatement ps = conn.prepareStatement(
                    "SELECT c.department, AVG(g.score) AS avg_score " +
                            "FROM Grade g JOIN Course c " +
                            "ON g.course_id = c.course_id " +
                            "GROUP BY c.department");

            ResultSet rs = ps.executeQuery();
            StringBuilder sb = new StringBuilder();

            while (rs.next()) {
                if (sb.length() > 0)
                    sb.append(";");

                sb.append(rs.getString("department"))
                        .append(":")
                        .append(rs.getDouble("avg_score"));
            }

            return sb.toString();

        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    /**
     * TODO: Find all the students that have taken most number of courses
     */
    public String getAllStudentsWithMostCourses() {
        try {
            Random rand = new Random();
            int fid = rand.nextInt(numFragments);

            Connection conn = connectionPool.get(fid);

            PreparedStatement ps = conn.prepareStatement(
                    "SELECT student_id " +
                            "FROM Grade " +
                            "GROUP BY student_id " +
                            "HAVING COUNT(course_id) = (" +
                            "   SELECT MAX(cnt) FROM (" +
                            "       SELECT COUNT(course_id) AS cnt " +
                            "       FROM Grade " +
                            "       GROUP BY student_id" +
                            "   ) sub" +
                            ")");

            ResultSet rs = ps.executeQuery();
            StringBuilder sb = new StringBuilder();

            while (rs.next()) {
                if (sb.length() > 0)
                    sb.append(";");
                sb.append(rs.getString("student_id"));
            }

            return sb.toString();

        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    public void closeConnections() {
        try {
            for (Connection conn : connectionPool.values()) {
                if (conn != null)
                    conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
