package edu.uci.ics.tippers.caching.workload;

import edu.uci.ics.tippers.generation.query.QueryGen;
import edu.uci.ics.tippers.persistor.PolicyPersistor;
import edu.uci.ics.tippers.dbms.mysql.MySQLConnectionManager;
import edu.uci.ics.tippers.execution.experiments.performance.QueryPerformance;
import edu.uci.ics.tippers.generation.policy.WiFiDataSet.PolicyUtil;
import edu.uci.ics.tippers.generation.query.QueryGen;
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
import java.sql.Connection;

/*
Write queries for space-usage scenario.
You can start with copy-paste the CQueryGenAC and start making edits on it.
 */
public class CQueryGenSU extends QueryGen {
    private Connection connection;

    Random r;
    PolicyPersistor polper;
    PolicyUtil pg;
    CUserGen cug;
    int flag;

    private List<Integer> user_ids;
    private List<String> locations;
    private List<String> user_groups;
    Timestamp start_beg, start_fin;
    private List<Integer> hours;
    private List<Integer> numUsers;

    public CQueryGenSU(){
        connection = MySQLConnectionManager.getInstance().getConnection();
        r = new Random();
        polper = PolicyPersistor.getInstance();
        pg = new PolicyUtil();
        cug = new CUserGen(1);

        this.user_ids = new ArrayList<>();
        this.start_beg = pg.getDate("MIN");
        this.start_fin = pg.getDate("MAX");

        //Added 12 new locations that are not used in Attendance Control
        this.locations = new ArrayList<>(Arrays.asList("3142-clwa-2019","3142-clwa-2039","3142-clwa-2051","3142-clwa-2059",
                "3142-clwa-2065","3142-clwa-2099","3142-clwa-2209","3142-clwa-2219","3142-clwa-2231","3143-clwa-3039",
                "3143-clwa-3051","3143-clwa-3059","3143-clwa-3099","3143-clwa-3209","3143-clwa-3219","3143-clwa-3231",
                "3144-clwa-4019","3144-clwa-4039","3144-clwa-4051","3144-clwa-4059","3144-clwa-4065","3144-clwa-4099",
                "3144-clwa-4209","3144-clwa-4219","3144-clwa-4231","3145-clwa-5019","3145-clwa-5039","3145-clwa-5051",
                "3145-clwa-5059","3145-clwa-5065","3145-clwa-5099","3145-clwa-5209","3145-clwa-5219","3145-clwa-5231",
                "3146-clwa-6011","3146-clwa-6029","3146-clwa-6049","3146-clwa-6131","3146-clwa-6217","3141-clwa-1100",
                "3141-clwa-1412","3141-clwa-1431","3141-clwa-1433","3141-clwb-1100","3141-clwc-1100","3141-clwd-1100",
                "3141-clwe-1100","3146-clwa-6219","3146-clwa-6122","3143-clwa-3019","3143-clwa-3065"));

        this.user_groups = new ArrayList<>(Arrays.asList("3142-clwa-2019","3142-clwa-2039","3142-clwa-2051","3142-clwa-2059",
                "3142-clwa-2065","3142-clwa-2099","3142-clwa-2209","3142-clwa-2219","3142-clwa-2231","3143-clwa-3039",
                "3143-clwa-3051","3143-clwa-3059","3143-clwa-3099","3143-clwa-3209","3143-clwa-3219","3143-clwa-3231",
                "3144-clwa-4019","3144-clwa-4039","3144-clwa-4051","3144-clwa-4059","3144-clwa-4065","3144-clwa-4099",
                "3144-clwa-4209","3144-clwa-4219","3144-clwa-4231","3145-clwa-5019","3145-clwa-5039","3145-clwa-5051",
                "3145-clwa-5059","3145-clwa-5065","3145-clwa-5099","3145-clwa-5209","3145-clwa-5219","3145-clwa-5231",
                "3146-clwa-6011","3146-clwa-6029","3146-clwa-6049","3146-clwa-6131","3146-clwa-6217","3141-clwa-1100",
                "3141-clwa-1412","3141-clwa-1431","3141-clwa-1433","3141-clwb-1100","3141-clwc-1100","3141-clwd-1100",
                "3141-clwe-1100","3146-clwa-6219","3146-clwa-6122","3143-clwa-3019","3143-clwa-3065"));
        user_groups.addAll(locations);
        //Added all user groups that are unused in Attendance Control
        user_groups.addAll(new ArrayList<>(Arrays.asList("faculty", "undergrad", "graduate", "staff", "visitor")));
        hours = new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5, 7, 10, 12, 15, 17, 20, 23));
        numUsers = new ArrayList<Integer>(Arrays.asList(10,50,100,150,200,250,300,350,400,420));


    }

    @Override
    public List<QueryStatement> createQuery1(int queryCount) {
        List<QueryStatement> queries = new ArrayList<>();
        int duration = 1440;

        for (int numQ = 0; numQ < queryCount; numQ++) {
            // Generate query without considering selectivity
            int locs = r.nextInt(locations.size());
            TimeStampPredicate tsPred = new TimeStampPredicate(pg.getDate("MIN"), 89, "00:00", duration);
            String query = String.format("start_date >= \"%s\" AND start_date <= \"%s\" ",
                    tsPred.getStartDate().toString(), tsPred.getEndDate().toString());
            query += String.format("and start_time >= \"%s\" AND start_time <= \"%s\" ",
                    tsPred.getStartTime().toString(), tsPred.getEndTime().toString());
            List<String> locPreds = new ArrayList<>();
            if (locs > 0) {
                for (int predCount = 0; predCount < locs; predCount++) {
                    locPreds.add(String.valueOf(locations.get(new Random().nextInt(locations.size()))));
                }
            } else {
                // Handle the case where locs is empty
                // For example, you could add a default location:
                locPreds.add("3142-clwa-2209");
            }
            query += "AND location_id IN (";
            query += locPreds.stream().map(item -> "\"" + item + "\"").collect(Collectors.joining(", "));
            query += ")";
            queries.add(new QueryStatement(query, 0, new Timestamp(System.currentTimeMillis())));
            duration += 60;
        }
        return queries;
    }

    @Override
    public List<QueryStatement> createQuery2(int queryCount) {
        List<QueryStatement> queries = new ArrayList<>();
        List<CUserGen.User> users = cug.retrieveUserDataForSU();
        for (CUserGen.User user: users) {
            user_ids.add(user.getId());
        }

        for (int numQ = 0; numQ < queryCount; numQ++) {
            // Generate query without considering selectivity
            int userCount = numUsers.get(new Random().nextInt(numUsers.size()));
            if (userCount == 0) {
                // Ensure there is at least one user
                userCount = 1;
            }
            TimeStampPredicate tsPred = new TimeStampPredicate(pg.getDate("MIN"), 89, "00:00", 1439);
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
            queries.add(new QueryStatement(query, 1, new Timestamp(System.currentTimeMillis())));
        }
        return queries;
    }

    @Override
    public List<QueryStatement> createQuery4() {
        List<QueryStatement> queries = new ArrayList<>();
        for (int j = 0; j < 200; j++) {
            // Generate query without considering selectivity
            TimeStampPredicate tsPred = new TimeStampPredicate(pg.getDate("MIN"), 89, "00:00", 1439);
            String query = String.format("SELECT location_id, COUNT(*) FROM PRESENCE WHERE start_time >= \"%s\" " +
                            "AND start_time <= \"%s\" GROUP BY location_id",
                    tsPred.getStartTime().toString(), tsPred.getEndTime().toString());
            queries.add(new QueryStatement(query, 3, new Timestamp(System.currentTimeMillis())));
        }
        return queries;
    }
    public List<QueryStatement> createQuery1(List<String> selTypes, int numOfQueries){return null;};

    public List<QueryStatement> createQuery2(List<String> selTypes, int numOfQueries){return null;};

    public List<QueryStatement> createQuery3(List<String> selTypes, int numOfQueries){return null;};

    public void runExperiment() {
        CQueryGenSU cqg = new CQueryGenSU();
        QueryPerformance e = new QueryPerformance();
        boolean[] templates = {true, true, false, false};
        int numOfQueries = 182180;
        String querier;
        List<QueryStatement> queries = cqg.constructWorkload(templates, numOfQueries);
        for (QueryStatement query : queries) {
            System.out.println(query.toString());
            querier = e.runExperiment(query);
            System.out.println(querier);
        }
        cqg.insertQuery(queries);
        System.out.println();
    }
    // TODO
}