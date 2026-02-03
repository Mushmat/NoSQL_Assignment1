package fragment;
import java.sql.*;
import java.util.*;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
public class FragmentClient {

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
            int fid = router.getFragmentId(studentId);
            Connection conn = connectionPool.get(fid);

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

    /**
     * TODO: Route the grade to the correct shard and execute the INSERT.
     */
    public void insertGrade(String studentId, String courseId, int score) {
        try {
            // Your code here
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void updateGrade(String studentId, String courseId, int newScore) {
        try {
		// Your code here:
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteStudentFromCourse(String studentId, String courseId) {
        try {
	// Your code here:
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * TODO: Fetch the student's name and email.
     */
    public String getStudentProfile(String studentId) {
        try {
            // Your code here
            return null; 
            
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
            // Your code here
            return null;

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
            // Your code here
            return null;

        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    public void closeConnections() {
        
    }
}
