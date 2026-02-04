package fragment;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
public class FragmentClient {
    private static final String USER = "postgres";
    private static final String PASSWORD = "123456";
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
                String url = "jdbc:postgresql://localhost:5432/frag" + i;
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
            int fragment_id = router.getFragmentId(studentId);
            Connection conn = connectionPool.get(fragment_id);

            PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO Student (student_id, name, age, email) VALUES (?, ?, ?, ?)"
            );

            ps.setString(1, studentId);
            ps.setString(2, name);
            ps.setInt(3, age);
            ps.setString(4, email);
            ps.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Written by Gautam IMT2023082
    public void insertGrade(String studentId, String courseId, int score) {
        try {
            int fragment_id = router.getFragmentId(studentId);
            Connection conn = connectionPool.get(fragment_id);

            PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO Grade (student_id, course_id, score) " +
                "VALUES (?, ?, ?)"
            );

            ps.setString(1, studentId);
            ps.setString(2, courseId);
            ps.setInt(3, score);

            ps.executeUpdate();
        }

        catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Written by Gautam IMT2023082
    public void updateGrade(String studentId, String courseId, int newScore) {
        try {
            int fragment_id = router.getFragmentId(studentId);
            Connection conn = connectionPool.get(fragment_id);

            PreparedStatement ps = conn.prepareStatement(
                "UPDATE Grade SET score = ? " +
                "WHERE student_id = ? AND course_id = ?"
            );

            ps.setInt(1, newScore);
            ps.setString(2, studentId);
            ps.setString(3, courseId);

            ps.executeUpdate();
        }

        catch (Exception e) {
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
                "WHERE student_id = ? AND course_id = ?"
            );

            ps.setString(1, studentId);
            ps.setString(2, courseId);

            ps.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getStudentProfile(String studentId) {
        try {
            int fid = router.getFragmentId(studentId);
            Connection conn = connectionPool.get(fid);

            PreparedStatement ps = conn.prepareStatement(
                "SELECT name, email FROM Student " +
                "WHERE student_id = ?"
            );

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
            Map<String, List<Integer>> map = new HashMap<>();

            for (Connection conn : connectionPool.values()) {
                PreparedStatement ps = conn.prepareStatement(
                    "SELECT c.department, g.score " +
                    "FROM Grade g JOIN Course c ON g.course_id = c.course_id"
                );
                ResultSet rs = ps.executeQuery();

                while (rs.next()) {
                    map.computeIfAbsent(rs.getString(1), k -> new ArrayList<>())
                    .add(rs.getInt(2));
                }
            }

            StringBuilder sb = new StringBuilder();
            for (String dept : map.keySet()) {
                double avg = map.get(dept).stream().mapToInt(i -> i).average().orElse(0);
                sb.append(dept)
                .append(":")
                .append(String.format("%.1f", avg))
                .append(";");
            }

            return sb.length() == 0 ? "" : sb.substring(0, sb.length() - 1);

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
        Map<String, Integer> count = new HashMap<>();

        for (Connection conn : connectionPool.values()) {
            PreparedStatement ps = conn.prepareStatement(
                "SELECT student_id, COUNT(*) cnt FROM Grade GROUP BY student_id"
            );
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                count.merge(rs.getString(1), rs.getInt(2), Integer::sum);
            }
        }

        int max = count.values().stream().max(Integer::compare).orElse(0);

        StringBuilder sb = new StringBuilder();
        for (String s : count.keySet()) {
            if (count.get(s) == max) sb.append(s).append(";");
        }

        return sb.length() == 0 ? "" : sb.substring(0, sb.length() - 1);

    } catch (Exception e) {
        e.printStackTrace();
        return "ERROR";
    }
}

    public void closeConnections() {
        try {
            for (Connection conn : connectionPool.values()) {
                if (conn != null) conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}