package edu.uci.ics.tippers.dbms.mysql;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by cygnus on 10/29/17.
 */
public class MySQLConnectionManager {

    private static MySQLConnectionManager _instance = new MySQLConnectionManager();
    private Properties props;
    private static String SERVER;
    private static String PORT;
    private static String DATABASE;
    private static String USER;
    private static String PASSWORD;
    private static Connection connection;

    private static final int MAX_CONNECTIONS =1000;

    private static int countConnection;

    private MySQLConnectionManager() {
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream(String.valueOf
                    (Paths.get(PolicyConstants.CRED_HQ + PolicyConstants.DBMS_LOCATION.toLowerCase(),
                            PolicyConstants.DBMS_CREDENTIALS.toLowerCase())) + ".properties");
            props = new Properties();
            props.load(inputStream);

            SERVER = props.getProperty("server");
            PORT = props.getProperty("port");
            DATABASE = props.getProperty("database");
            USER = props.getProperty("user");
            PASSWORD = props.getProperty("password");

        } catch (IOException ie) {
            ie.printStackTrace();
        }
    }

    public static MySQLConnectionManager getInstance() {
        return _instance;
    }


    public Connection getConnection() throws PolicyEngineException {
        if (connection != null && countConnection<MAX_CONNECTIONS){
            countConnection++;
            System.out.println("Connection count: " + countConnection);
            return connection;
        }
        try {
            connection = DriverManager.getConnection(
                    String.format("jdbc:mysql://%s:%s/%s?useLegacyDatetimeCode=false&serverTimezone=America/Los_Angeles&rewriteBatchedStatements=true&enabledTLSProtocols=TLSv1.2",
                            SERVER, PORT, DATABASE), USER, PASSWORD);
            System.out.println("--- Connected to " + DATABASE + " on server " + SERVER + "---");
            countConnection = 1;
            System.out.println("Connection count: " + countConnection);
            return connection;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new PolicyEngineException("Error Connecting to MySQL");
        }
    }
}
