import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseUtils {
    private static String DBMS_URL = "jdbc:mysql://";
    private static String DBMS_CLASSPATH = "com.mysql.cj.jdbc.Driver";

    /**
     * mở đến nối đến database
     * @return Connection
     */
    public static Connection openConnection(ConnectionInformation ci) throws Exception {
        Connection c = null;
        String url = DBMS_URL + ci.getServer() + "/" + ci.getDatabaseName();
        Class.forName(DBMS_CLASSPATH);
        c = DriverManager.getConnection(url, ci.getUsername(), ci.getPassword());
        return c;
    }

    /**
     * đóng kết nối đến database
     * @param c Connection đến database
     * @throws Exception lỗi thực thi
     */
    public static void closeConnection(Connection c){
        try {
            c.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
