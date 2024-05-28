package edu.uci.ics.tippers.caching.workload;

import java.sql.*;
import edu.uci.ics.tippers.dbms.mysql.MySQLConnectionManager;
import java.util.*;

public class CUserGen {

    private Connection connection;

    public CUserGen() {
        connection = MySQLConnectionManager.getInstance().getConnection();
    }

    public List<User> retrieveUserData() {
        List<User> users = new ArrayList<>();
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT id, user_id, user_profile, user_group FROM app_user WHERE user_profile IN ('graduate', 'undergrad', 'faculty') and user_group NOT IN ('3143-clwa-3019', '3146-clwa-6122', '3143-clwa-3065', '3146-clwa-6219')");
            int count = 0;
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String userId = resultSet.getString("user_id");
                String userProfile = resultSet.getString("user_profile");
                String userGroup = resultSet.getString("user_group");
                User user = new User(id, userId, userProfile, userGroup);
                users.add(user);
                count++;
                System.out.println("Entry #" + count + ": " + user);
            }
            System.out.println("Total number of entries: " + count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return users;
    }


    public void printUserData(List<User> users) {
        System.out.println("User Data:");
        for (User user : users) {
            System.out.println(user);
        }
        System.out.println("Total number of entries: " + users.size());
    }

    public void runExperiment() {
        CUserGen cUserGen = new CUserGen();
        List<User> users = cUserGen.retrieveUserData();
        cUserGen.printUserData(users);
    }

    public static class User {
        private int id;
        private String userId;
        private String userProfile;
        private String userGroup;

        public User(int id, String userId, String userProfile, String userGroup) {
            this.id = id;
            this.userId = userId;
            this.userProfile = userProfile;
            this.userGroup = userGroup;
        }

        // Getters and setters
        public int getId() {
            return id;
        }
        public String getUserId(){ return userId; }
        public String getUserProfile(){ return userProfile; }
        public String getUserGroup(){ return userGroup; }

        @Override
        public String toString() {
            return "User{" +
                    "id=" + id +
                    ", userId='" + userId + '\'' +
                    ", userProfile='" + userProfile + '\'' +
                    ", userGroup='" + userGroup + '\'' +
                    '}';
        }
    }
}
