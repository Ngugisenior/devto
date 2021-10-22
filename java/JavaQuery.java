import java.sql.*;
import com.sap.db.jdbc.Driver;
public class JavaQuery {
    public static void main(String[] argv) {
        System.out.println("Java version: " + com.sap.db.jdbc.Driver.getJavaVersion());
        System.out.println("Minimum supported Java version and SAP driver version number: " + com.sap.db.jdbc.Driver.getVersionInfo());
        Connection connection = null;
        try {  
            connection = DriverManager.getConnection(  
                //Option 1, retrieve the connection parameters from the hdbuserstore
                //The below URL gets the host, port and credentials from the hdbuserstore.
                "jdbc:sap://dummy_host:0/?KEY=pyraxuserkey&encrypt=true&validateCertificate=false");

                //Option2, specify the connection parameters
                //"jdbc:sap://10.11.123.134:39015/?encrypt=true&validateCertificate=false", "User1", "Password1");

                //As of SAP HANA Client 2.6, connections on port 443 enable encryption by default
                //validateCertificate should be set to false when connecting
                //to an SAP HANA, express edition instance that uses a self-signed certificate.

                //As of SAP HANA Client 2.7, it is possible to direct trace info to stdout or stderr
                //"jdbc:sap://10.11.123.134:39015/?encrypt=true&validateCertificate=false&traceFile=stdout&traceOptions=CONNECTIONS", "User1", "Password1");

        }
        catch (SQLException e) {
            System.err.println("Connection Failed:");
            System.err.println(e);
            return;
        }
        if (connection != null) {
            try {
                System.out.println("Connection to HANA successful!");
                Statement stmt = connection.createStatement();
                ResultSet resultSet = stmt.executeQuery("SELECT TITLE, FIRSTNAME, NAME from HOTEL.CUSTOMER;");
                while (resultSet.next()) {
                    String title = resultSet.getString(1);
                    String firstName = resultSet.getString(2);
                    String lastName = resultSet.getString(3);
                    System.out.println(title + " " + firstName + " " + lastName);
                }
            }
            catch (SQLException e) {
                System.err.println("Query failed!");
            }
        }
    }
}
