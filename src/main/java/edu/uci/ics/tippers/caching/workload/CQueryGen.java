package edu.uci.ics.tippers.caching.workload;

import edu.uci.ics.tippers.dbms.mysql.MySQLConnectionManager;
import edu.uci.ics.tippers.generation.policy.WiFiDataSet.PolicyUtil;
import edu.uci.ics.tippers.generation.query.QueryGen;
import edu.uci.ics.tippers.generation.query.WiFiDataSet.WiFiDataSetQueryGeneration;
import edu.uci.ics.tippers.model.policy.TimeStampPredicate;
import edu.uci.ics.tippers.model.query.QueryStatement;
import edu.uci.ics.tippers.persistor.PolicyPersistor;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class CQueryGen extends QueryGen {
    private Connection connection;

    Random r;
    PolicyPersistor polper;
    PolicyUtil pg;

    private List<Integer> user_ids;
    private List<String> locations;
    private List<String> user_groups;
    Timestamp start_beg, start_fin;
    private List<Integer> hours;
    private List<Integer> numUsers;

    public CQueryGen(){
        connection = MySQLConnectionManager.getInstance().getConnection();
        r = new Random();
        polper = PolicyPersistor.getInstance();
        pg = new PolicyUtil();

        this.user_ids = pg.getAllUsers(false);
        this.locations = pg.getAllLocations();
        this.start_beg = pg.getDate("MIN");
        this.start_fin = pg.getDate("MAX");
        this.user_groups = new ArrayList<>(Arrays.asList("3142-clwa-2209", "3144-clwa-4051", "3146-clwa-6131"));
        user_groups.addAll(locations);
        user_groups.addAll(new ArrayList<>(Arrays.asList("faculty", "staff", "undergrad", "graduate", "visitor")));
        hours = new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5, 7, 10, 12, 15, 17, 20, 23));
        numUsers = new ArrayList<Integer>(Arrays.asList(1000, 2000, 3000, 5000, 10000, 11000, 12000, 13000, 14000, 15000));

    }

    @Override
    public List<QueryStatement> createQuery1(int queryCount) {
        List<QueryStatement> queries = new ArrayList<>();

        for (int numQ = 0; numQ < queryCount; numQ++) {
            // Generate query without considering selectivity
            int locs = r.nextInt(locations.size());
            TimeStampPredicate tsPred = new TimeStampPredicate(pg.getDate("MIN"), 0, "00:00", 60);
            String query = String.format("start_date >= \"%s\" AND start_date <= \"%s\" ",
                    tsPred.getStartDate().toString(), tsPred.getEndDate().toString());
            query += String.format("and start_time >= \"%s\" AND start_time <= \"%s\" ",
                    tsPred.getStartTime().toString(), tsPred.getEndTime().toString());
            List<String> locPreds = new ArrayList<>();
            for (int predCount = 0; predCount < locs; predCount++) {
                locPreds.add(String.valueOf(locations.get(new Random().nextInt(locations.size()))));
            }
            query += "AND location_id IN (";
            query += locPreds.stream().map(item -> "\"" + item + "\"").collect(Collectors.joining(", "));
            query += ")";
            queries.add(new QueryStatement(query, 1, new Timestamp(System.currentTimeMillis())));
        }
        return queries;
    }

    @Override
    public List<QueryStatement> createQuery2(int queryCount) {
        List<QueryStatement> queries = new ArrayList<>();

        for (int numQ = 0; numQ < queryCount; numQ++) {
            // Generate query without considering selectivity
            int userCount = numUsers.get(new Random().nextInt(numUsers.size()));
            TimeStampPredicate tsPred = new TimeStampPredicate(pg.getDate("MIN"), 0, "00:00", 300);
            String query = String.format("start_date >= \"%s\" AND start_date <= \"%s\" ",
                    tsPred.getStartDate().toString(), tsPred.getEndDate().toString());
            query += String.format("and start_time >= \"%s\" AND start_time <= \"%s\" ",
                    tsPred.getStartTime().toString(), tsPred.getEndTime().toString());
            List<Integer> userPreds = new ArrayList<>();
            for (int predCount = 0; predCount < userCount; predCount++) {
                userPreds.add(user_ids.get(new Random().nextInt(user_ids.size())));
            }
            query += "AND user_id IN (";
            query += userPreds.stream().map(String::valueOf).collect(Collectors.joining(", "));
            query += ")";
            queries.add(new QueryStatement(query, 2, new Timestamp(System.currentTimeMillis())));
        }
        return queries;
    }

    @Override
    public List<QueryStatement> createQuery3(int queryNum) {
        List<QueryStatement> queries = new ArrayList<>();
        Random r = new Random();
        String user_group = "staff";
        String full_query = String.format("Select PRESENCE.user_id, PRESENCE.location_id, PRESENCE.start_date, " +
                "PRESENCE.start_time, PRESENCE.user_group, PRESENCE.user_profile  " +
                "from PRESENCE, USER_GROUP_MEMBERSHIP " +
                "where USER_GROUP_MEMBERSHIP.user_group_id = \"%s\" AND PRESENCE.user_id = USER_GROUP_MEMBERSHIP.user_id " +
                "AND ", user_group);
        List<QueryStatement> select_queries = createQuery1(queryNum);
        for (QueryStatement qs: select_queries) {
            queries.add(new QueryStatement(full_query + qs.getQuery(), 3, new Timestamp(System.currentTimeMillis())));
        }
        return queries;
    }

    @Override
    public List<QueryStatement> createQuery4() {
        List<QueryStatement> queries = new ArrayList<>();
        for (int j = 0; j < 200; j++) {
            // Generate query without considering selectivity
            TimeStampPredicate tsPred = new TimeStampPredicate(pg.getDate("MIN"), 0, "00:00", 7 * j);
            String query = String.format("SELECT location_id, COUNT(*) FROM PRESENCE WHERE start_time >= \"%s\" " +
                            "AND start_time <= \"%s\" GROUP BY location_id",
                    tsPred.getStartTime().toString(), tsPred.getEndTime().toString());
            queries.add(new QueryStatement(query, 4, new Timestamp(System.currentTimeMillis())));
        }
        return queries;
    }

    public List<QueryStatement> createQuery1(List<String> selTypes, int numOfQueries){return null;};

    public List<QueryStatement> createQuery2(List<String> selTypes, int numOfQueries){return null;};

    public List<QueryStatement> createQuery3(List<String> selTypes, int numOfQueries){return null;};

    public static void main(String[] args) {
        CQueryGen cqg = new CQueryGen();
        boolean[] templates = {true, true, true, true};
        int numOfQueries = 4;
        List<QueryStatement> queries = cqg.constructWorkload(templates, numOfQueries);
        for (QueryStatement query : queries) {
            System.out.println(query.toString());
        }
        System.out.println();
    }

}
