package fragment;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class FragmentClient {
    private static final String DB_USER = "postgres";
    private static final String DB_PASS = "123456";
    private static final String BASE_URL = "jdbc:postgresql://localhost:5432/frag";

    private final Map<Integer, Connection> shardMap;
    private final Router shardRouter;
    private final int totalFragments;

    public FragmentClient(int numFragments) {
        this.totalFragments = numFragments;
        this.shardRouter = new Router(numFragments);
        this.shardMap = new HashMap<>();
    }

    /**
     * Establishes connections to all database shards.
     */
    public void setupConnections() {
        try {
            for (int i = 0; i < totalFragments; i++) {
                String connectionUrl = BASE_URL + i;
                Connection conn = DriverManager.getConnection(connectionUrl, DB_USER, DB_PASS);
                shardMap.put(i, conn);
            }
        } catch (SQLException e) {
            System.err.println("Failed to initialize database connections: " + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Routes and inserts a student record into the appropriate shard.
     */
    public void insertStudent(String id, String name, int age, String email) {
        String sql = "INSERT INTO Student (student_id, name, age, email) VALUES (?, ?, ?, ?) " +
                     "ON CONFLICT (student_id) DO NOTHING";
        
        executeShardUpdate(id, sql, id, name, age, email);
    }

    public void insertGrade(String studentId, String courseId, int score) {
        String sql = "INSERT INTO Grade (student_id, course_id, score) VALUES (?, ?, ?) " +
                     "ON CONFLICT (student_id, course_id) DO NOTHING";
        
        executeShardUpdate(studentId, sql, studentId, courseId, score);
    }

    public void updateGrade(String studentId, String courseId, int newScore) {
        String sql = "UPDATE Grade SET score = ? WHERE student_id = ? AND course_id = ?";
        
        executeShardUpdate(studentId, sql, newScore, studentId, courseId);
    }

    public void deleteStudentFromCourse(String studentId, String courseId) {
        String sql = "DELETE FROM Grade WHERE student_id = ? AND course_id = ?";
        
        executeShardUpdate(studentId, sql, studentId, courseId);
    }

    /**
     * Retrieves student basic info from the specific shard.
     */
    public String getStudentProfile(String studentId) {
        String sql = "SELECT name, email FROM Student WHERE student_id = ?";
        int shardId = shardRouter.getFragmentId(studentId);

        try (PreparedStatement ps = shardMap.get(shardId).prepareStatement(sql)) {
            ps.setString(1, studentId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return String.format("%s,%s", rs.getString("name"), rs.getString("email"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * Aggregates average scores across all shards grouped by department.
     */
    public String getAvgScoreByDept() {
        Map<String, List<Integer>> deptScores = new HashMap<>();
        String query = "SELECT c.department, g.score FROM Grade g " +
                       "JOIN Course c ON g.course_id = c.course_id";

        try {
            for (Connection conn : shardMap.values()) {
                try (PreparedStatement ps = conn.prepareStatement(query);
                     ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        deptScores.computeIfAbsent(rs.getString(1), k -> new ArrayList<>())
                                  .add(rs.getInt(2));
                    }
                }
            }

            return deptScores.entrySet().stream()
                .map(entry -> {
                    double avg = entry.getValue().stream().mapToInt(i -> i).average().orElse(0);
                    return String.format("%s:%.1f", entry.getKey(), avg);
                })
                .collect(Collectors.joining(";"));

        } catch (SQLException e) {
            return "ERROR";
        }
    }

    /**
     * Identifies students with the highest enrollment count across all shards.
     */
    public String getAllStudentsWithMostCourses() {
        Map<String, Integer> enrollmentCounts = new HashMap<>();
        String query = "SELECT student_id, COUNT(*) as cnt FROM Grade GROUP BY student_id";

        try {
            for (Connection conn : shardMap.values()) {
                try (PreparedStatement ps = conn.prepareStatement(query);
                     ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        enrollmentCounts.merge(rs.getString("student_id"), rs.getInt("cnt"), Integer::sum);
                    }
                }
            }

            if (enrollmentCounts.isEmpty()) return "";

            int maxCourses = Collections.max(enrollmentCounts.values());

            return enrollmentCounts.entrySet().stream()
                .filter(e -> e.getValue() == maxCourses)
                .map(Map.Entry::getKey)
                .collect(Collectors.joining(";"));

        } catch (SQLException e) {
            return "ERROR";
        }
    }

    /**
     * Generic helper to handle shard-specific updates/inserts/deletes.
     */
    private void executeShardUpdate(String routingKey, String sql, Object... params) {
        int shardId = shardRouter.getFragmentId(routingKey);
        try (PreparedStatement ps = shardMap.get(shardId).prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void closeConnections() {
        shardMap.values().forEach(conn -> {
            try {
                if (conn != null && !conn.isClosed()) conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }
}
