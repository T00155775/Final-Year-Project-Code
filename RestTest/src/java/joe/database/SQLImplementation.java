package joe.database;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bson.types.ObjectId;

/**
 *
 * @author Joe O Flaherty
 */
public class SQLImplementation implements IRepository {

    long totalTime = 0;
    long averageTime = 0;
    long max = 0;
    long min = 0;

    String sname;
    String snumber;
    String scourse;
    String createddate;
    long duration;

    String userName = "root";
    String passWord = "xc4wbbbb";
    String sql;
    Statement stmt = null;
    PreparedStatement preparedStmt = null;

    //Azure statsD client
    StatsDClient statsdSave = new NonBlockingStatsDClient("Save", "137.135.253.106", 80);
    StatsDClient statsdFind = new NonBlockingStatsDClient("Find", "137.135.253.106", 80);
    StatsDClient statsdDelete = new NonBlockingStatsDClient("Delete", "137.135.253.106", 80);
    StatsDClient statsdFindOne = new NonBlockingStatsDClient("Find_One", "137.135.253.106", 80);

    //VMWare statsD client
    //StatsDClient statsd = new NonBlockingStatsDClient("Save", "192.168.45.130", 8125);
    @SuppressWarnings({"ConvertToTryWithResources", "CallToPrintStackTrace"})
    public String find() {

        String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(SQLImplementation.class.getName()).log(Level.SEVERE, null, ex);
        }
        String DB_URL = "jdbc:mysql://localhost:3306/testws";
        Connection conn = null;

        String result = "";
        try {

            //STEP 2: Register JDBC driver
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(DB_URL, userName, passWord);
            if (conn != null) {
                System.out.println("Connected to the database testws");
            }

            stmt = conn.createStatement();

            sql = "SELECT * FROM studentws";

            long startTime = System.nanoTime();
            ResultSet rs = stmt.executeQuery(sql);
            long endTime = System.nanoTime();
            duration = endTime - startTime;

            statsdFind.incrementCounter("MySQL Find Counter");
            statsdFind.recordGaugeValue("MySQL Find Gauge", 100);
            statsdFind.recordExecutionTime("MySQL Find Timer", duration);
            statsdFind.recordSetEvent("MySQL Find SetEvent", "One");

            while (rs.next()) {

                sname = rs.getString("sname");
                snumber = rs.getString("snumber");
                scourse = rs.getString("scourse");
                createddate = rs.getDate("createddate").toString();
                result += sname + " " + snumber + " " + scourse + " " + createddate + "\n";

            }

            rs.close();
            stmt.close();
            conn.close();
        } catch (SQLException | ClassNotFoundException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } finally {
            //finally block used to close resources
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException se2) {
            }// nothing we can do
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }//end finally try
        }//end try
        //return sname + " " + snumber + " " + scourse + " " + createddate;
        return result;
    }

    @SuppressWarnings("CallToPrintStackTrace")
    public String save() {

        String DB_URL = "jdbc:mysql://localhost:3306/testws";
        Connection conn = null;
        try {

            Class.forName("com.mysql.jdbc.Driver");

            conn = DriverManager.getConnection(DB_URL, userName, passWord);

            stmt = conn.createStatement();
            for (int i = 1; i <= 100; i++) {

                sql = "INSERT INTO studentws (sname,snumber,scourse,createddate)"
                        + " values (?, ?, ?, ?)";

                // stmt.executeUpdate(sql);
                //Calendar calendar = Calendar.getInstance();
                //java.sql.Date cDate = new java.sql.Date(calendar.getTime().getTime());
                java.sql.Timestamp date = new java.sql.Timestamp(new java.util.Date().getTime());

                preparedStmt = conn.prepareStatement(sql);
                preparedStmt.setString(1, "Tom" + i);
                preparedStmt.setString(2, "3" + i);
                preparedStmt.setString(3, "Computing");
                preparedStmt.setTimestamp(4, date);

                //long duration;
                max = duration;
                long startTime = System.nanoTime();
                //stmt.executeUpdate(sql);
                //preparedStmt.execute();
                preparedStmt.executeUpdate();
                long endTime = System.nanoTime();
                duration = endTime - startTime;
                min = duration;
                if (duration > max) {
                    max = duration;
                }
                duration = endTime - startTime;
                if (duration < min) {
                    min = duration;
                }
                totalTime += duration;

                statsdSave.incrementCounter("MySQL Save Counter");
                statsdSave.recordGaugeValue("MySQL Save Gauge", 100);
                statsdSave.recordExecutionTime("MySQL Save Timer", duration);
                statsdSave.recordSetEvent("MySQL Save SetEvent", "One");
                statsdSave.recordExecutionTime("MySQL Save Maximum", max);
                statsdSave.recordExecutionTime("MySQL Save Minimum", min);
            }
        } catch (SQLException | ClassNotFoundException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } finally {
            try {
                //finally block used to close resources
                conn.close();
            } catch (SQLException ex) {
                Logger.getLogger(SQLImplementation.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
                if (stmt != null) {
                    conn.close();
                }
            } catch (SQLException se) {
            }// do nothing
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }//end finally try
        }
        averageTime = totalTime / 100;
        statsdSave.recordExecutionTime("MySQL Save Total Time(100)", totalTime);
        statsdSave.recordExecutionTime("MySQL Save Average Time(100)", averageTime);

        return "MySQL Save Success";
    }

    public String findSingle(ObjectId Id) {
        return "";
    }

    public String findSpecific(String recordId) {

        String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(SQLImplementation.class.getName()).log(Level.SEVERE, null, ex);
        }
        String DB_URL = "jdbc:mysql://localhost:3306/testws";
        Connection conn = null;

        String result = "";
        try {

            //STEP 2: Register JDBC driver
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(DB_URL, userName, passWord);
            if (conn != null) {
                System.out.println("Connected to the database testws");
            }

            stmt = conn.createStatement();

            sql = "SELECT * FROM studentws Where sname =" + "'" + recordId + "'";
            // long duration;

            long startTime = System.nanoTime();
            ResultSet rs = stmt.executeQuery(sql);
            long endTime = System.nanoTime();
            duration = endTime - startTime;

            statsdFindOne.incrementCounter("MySQL Find_One Counter");
            statsdFindOne.recordGaugeValue("MySQL Find_One Gauge", 100);
            statsdFindOne.recordExecutionTime("MySQL Find_One Timer", duration);
            statsdFindOne.recordSetEvent("MySQL Find_One SetEvent", "One");

            while (rs.next()) {

                sname = rs.getString("sname");
                snumber = rs.getString("snumber");
                scourse = rs.getString("scourse");
                createddate = rs.getDate("createddate").toString();
                result += sname + " " + snumber + " " + scourse + " " + createddate + "\n";

            }

            rs.close();
            stmt.close();
            conn.close();
        } catch (SQLException | ClassNotFoundException se) {
        } finally {
            //finally block used to close resources
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException se2) {
            }// nothing we can do
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
            }//end finally try
        }//end try

        return result;
    }

    public String delete() {

        String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(SQLImplementation.class.getName()).log(Level.SEVERE, null, ex);
        }
        String DB_URL = "jdbc:mysql://localhost:3306/testws";
        Connection conn = null;

        try {

            //STEP 2: Register JDBC driver
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(DB_URL, userName, passWord);
            if (conn != null) {
                System.out.println("Connected to the database testws");
            }

            stmt = conn.createStatement();

            //sql = "DELETE FROM studentws WHERE scourse = "+"'Computing'";
            sql = "TRUNCATE TABLE studentws";
            long startTime = System.nanoTime();
            stmt.executeUpdate(sql);
            long endTime = System.nanoTime();
            duration = endTime - startTime;

            statsdDelete.incrementCounter("MySQL Delete Counter");
            statsdDelete.recordGaugeValue("MySQL Delete Gauge", 100);
            statsdDelete.recordExecutionTime("MySQL Delete Timer", duration);
            statsdDelete.recordSetEvent("MySQL Delete SetEvent", "One");

            stmt.close();
            conn.close();
        } catch (SQLException | ClassNotFoundException se) {
        } finally {
            //finally block used to close resources
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException se2) {
            }// nothing we can do
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
            }//end finally try
        }//end try

        return "MySQL all records deleted";
    }

}
