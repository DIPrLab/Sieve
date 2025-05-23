package edu.uci.ics.tippers.execution.experiments.performance;

import edu.uci.ics.tippers.caching.CachingAlgorithm;
import edu.uci.ics.tippers.caching.workload.CUserGen;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.QueryManager;
import edu.uci.ics.tippers.dbms.QueryResult;
import edu.uci.ics.tippers.dbms.mysql.MySQLConnectionManager;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.generation.policy.WiFiDataSet.PolicyUtil;
import edu.uci.ics.tippers.generation.query.QueryExplainer;
import edu.uci.ics.tippers.generation.query.WiFiDataSet.WiFiDataSetQueryGeneration;
import edu.uci.ics.tippers.persistor.GuardPersistor;
import edu.uci.ics.tippers.persistor.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.guard.GuardPart;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.query.QueryStatement;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

/**
 * Experiment 3 in the paper
 * Testing Sieve against baselines on MySQL
 */
public class QueryPerformance {


    PolicyPersistor polper;
    QueryExplainer queryExplainer;
    QueryManager queryManager;

    private static boolean QUERY_EXEC;
    private static boolean BASE_LINE_POLICIES;
    private static boolean BASELINE_UDF;
    private static boolean BASELINE_INDEX;
    private static boolean GUARD_POLICY_INLINE;
    private static boolean GUARD_UDF;
    private static boolean GUARD_INDEX;
    private static boolean QUERY_INDEX;
    private static boolean SIEVE_EXEC;
    private static boolean RESULT_CHECK;
    GuardPersistor guardPersistor;

    private static int NUM_OF_REPS;

    private Connection connection;

    private static String RESULTS_FILE;

    public QueryPerformance() {
        connection = MySQLConnectionManager.getInstance().getConnection();
        PolicyConstants.initialize();
        polper = PolicyPersistor.getInstance();
        queryExplainer = new QueryExplainer();
        queryManager = new QueryManager();
        this.guardPersistor = new GuardPersistor();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("config/execution/experiment3.properties");
            Properties props = new Properties();
            if (inputStream != null) {
                props.load(inputStream);
                QUERY_EXEC = Boolean.parseBoolean(props.getProperty("query_exec"));
                BASE_LINE_POLICIES = Boolean.parseBoolean(props.getProperty("baseline_policies"));
                GUARD_POLICY_INLINE = Boolean.parseBoolean(props.getProperty("guard_policies"));
                BASELINE_UDF = Boolean.parseBoolean(props.getProperty("baseline_udf"));
                BASELINE_INDEX = Boolean.parseBoolean(props.getProperty("baseline_index"));
                GUARD_UDF = Boolean.parseBoolean(props.getProperty("guard_udf"));
                GUARD_INDEX = Boolean.parseBoolean(props.getProperty("guard_index"));
                QUERY_INDEX = Boolean.parseBoolean(props.getProperty("query_index"));
                SIEVE_EXEC = Boolean.parseBoolean(props.getProperty("sieve_exec"));
                RESULT_CHECK = Boolean.parseBoolean(props.getProperty("resultCheck"));
                NUM_OF_REPS = Integer.parseInt(props.getProperty("num_repetitions"));
                RESULTS_FILE = props.getProperty("results_file");
            }
        } catch (IOException ie) {
            ie.printStackTrace();
        }
    }

    public String runBEPolicies(String querier, QueryStatement queryStatement, List<BEPolicy> bePolicies) {

        BEExpression beExpression = new BEExpression(bePolicies);
        StringBuilder resultString = new StringBuilder();
        resultString.append(querier).append(",")
//                .append(userProfile(Integer.parseInt(querier))).append(",")
                .append(queryStatement.getTemplate()).append(",")
                .append(queryStatement.getSelectivity()).append(",")
                .append(bePolicies.size()).append(",");

        QueryExplainer qe = new QueryExplainer();
        double querySel = qe.estimateSelectivity(queryStatement);
//        double querySel = 0;
        resultString.append(querySel).append(",");

        try {
            if (QUERY_EXEC) {
                QueryResult queryResult;
                if (queryStatement.getTemplate() == 3)
                    queryResult = queryManager.runTimedSubQuery(queryStatement.getQuery(), RESULT_CHECK);
                else
                    queryResult = queryManager.runTimedQueryWithOutSorting(queryStatement.getQuery(), true);
                if (queryResult.getResultCount() != 0) {
                    Duration runTime = Duration.ofMillis(0);
                    runTime = runTime.plus(queryResult.getTimeTaken());
                    System.out.println("Time taken by query alone " + runTime.toMillis());
                    long QUERY_EXECUTION_TIME = runTime.toMillis();
                    resultString.append(QUERY_EXECUTION_TIME).append(",");
//                mySQLQueryManager.increaseTimeout(runTime.toMillis()); //Updating timeout to query exec time + constant
                }
            } else resultString.append("NA").append(",");

            if (BASE_LINE_POLICIES) {
                System.out.println("Inside Baseline P");
                QueryResult tradResult;
                String polEvalQuery = "With polEval as ( Select * from PRESENCE where "
                        + beExpression.createQueryFromPolices() + "  )";
                if (queryStatement.getTemplate() == 3)
                    tradResult = queryManager.runTimedQueryExp(polEvalQuery + queryStatement.getQuery(), NUM_OF_REPS);
                else
                    tradResult = queryManager.runTimedQueryExp(polEvalQuery + "SELECT * from polEval where "
                            + queryStatement.getQuery(), NUM_OF_REPS);
                Duration runTime = Duration.ofMillis(0);
                runTime = runTime.plus(tradResult.getTimeTaken());
                resultString.append(runTime.toMillis()).append(",");
                System.out.println("Baseline inlining policies: No of Policies: " + beExpression.getPolicies().size() + " , Time: " + runTime.toMillis());
            } else resultString.append("NA").append(",");


            if (BASELINE_UDF) {
                QueryResult execResult;
                String udf_query = " and pcheck( " + querier + ", PRESENCE.user_id, PRESENCE.location_id, " +
                        "PRESENCE.start_date, PRESENCE.start_time, PRESENCE.user_profile, PRESENCE.user_group) = 1";
                if (queryStatement.getTemplate() == 3)
                    execResult = queryManager.runTimedQueryExp(queryStatement.getQuery() + udf_query, NUM_OF_REPS);
                else
                    execResult = queryManager.runTimedQueryExp("SELECT * from PRESENCE where "
                            + queryStatement.getQuery() + udf_query, NUM_OF_REPS);
                resultString.append(execResult.getTimeTaken().toMillis()).append(",");
                System.out.println("Baseline UDF: Time: " + execResult.getTimeTaken().toMillis());
            } else resultString.append("NA").append(",");


            if (BASELINE_INDEX) {
                QueryResult indResult;
                String polIndexQuery = "With polEval as ( " + beExpression.createIndexQuery() + " ) ";
                if (queryStatement.getTemplate() == 3)
                    indResult = queryManager.runTimedQueryExp(polIndexQuery + queryStatement.getQuery(), NUM_OF_REPS);
                else
                    indResult = queryManager.runTimedQueryExp(polIndexQuery + "SELECT * from polEval where "
                            + queryStatement.getQuery(), NUM_OF_REPS);
                resultString.append(indResult.getTimeTaken().toMillis()).append(",");
                System.out.println("Baseline Index: Time: " + indResult.getTimeTaken().toMillis());
            } else resultString.append("NA").append(",");

//            CachingAlgorithm ca =new CachingAlgorithm<>();
//            GuardExp GE = ca.SieveGG(querier);
            GuardPersistor guardPersistor = new GuardPersistor();
            GuardExp guardExp = guardPersistor.retrieveGuardExpression(querier, "user", bePolicies);
            if (guardExp.getGuardParts().isEmpty()) return "empty";
            resultString.append(guardExp.getGuardParts().size()).append(",");
            double guardTotalCard = guardExp.getGuardParts().stream().mapToDouble(GuardPart::getCardinality).sum();
            resultString.append(guardTotalCard).append(",");


            if (GUARD_POLICY_INLINE) {
                //TODO: Does not work for template 3
                Duration execTime = Duration.ofMillis(0);
                String guard_query_with_union = guardExp.inlineRewrite(true);
                String guard_query_with_or = guardExp.inlineRewrite(false);
                guard_query_with_union += "Select * from polEval where " + queryStatement.getQuery();
                guard_query_with_or += "Select * from polEval where " + queryStatement.getQuery();
                QueryResult execResult = queryManager.runTimedQueryExp(guard_query_with_union, NUM_OF_REPS);
                execTime = execTime.plus(execResult.getTimeTaken());
                resultString.append(execTime.toMillis()).append(",");
                System.out.println("Guard inline execution with union: " + " Time: " + execTime.toMillis());
                execResult = queryManager.runTimedQueryExp(guard_query_with_or, NUM_OF_REPS);
                execTime = execTime.plus(execResult.getTimeTaken());
                resultString.append(execTime.toMillis()).append(",");
                System.out.println("Guard inline execution with OR: " + " Time: " + execTime.toMillis());
            }
            if (GUARD_UDF) {
                //TODO: Does not work for template 3
                Duration execTime = Duration.ofMillis(0);
                String guard_query_with_union = guardExp.udfRewrite(true);
                String guard_query_with_or = guardExp.udfRewrite(false);
                guard_query_with_union += "Select * from polEval where " + queryStatement.getQuery();
                guard_query_with_or += "Select * from polEval where " + queryStatement.getQuery();
                QueryResult execResult = queryManager.runTimedQueryExp(guard_query_with_union, NUM_OF_REPS);
                execTime = execTime.plus(execResult.getTimeTaken());
                resultString.append(execTime.toMillis()).append(",");
                System.out.println("Guard udf execution with union: " + " Time: " + execTime.toMillis());
                execResult = queryManager.runTimedQueryExp(guard_query_with_or, NUM_OF_REPS);
                execTime = execTime.plus(execResult.getTimeTaken());
                resultString.append(execTime.toMillis());
                System.out.println("Guard udf execution with OR: " + " Time: " + execTime.toMillis());
            }
            if (GUARD_INDEX) {
                Duration execTime = Duration.ofMillis(0);
                String guard_hybrid_query = guardExp.inlineOrNot(true);
                if (queryStatement.getTemplate() == 3) {
                    guard_hybrid_query += queryStatement.getQuery().replace("PRESENCE", "polEval");
                } else
                    guard_hybrid_query += "Select * from polEval where " + queryStatement.getQuery();
                QueryResult execResult = queryManager.runTimedQueryExp(guard_hybrid_query, 1);
                execTime = execTime.plus(execResult.getTimeTaken());
                resultString.append(execTime.toMillis()).append(",");
                System.out.println("Guard Index execution : " + " Time: " + execTime.toMillis());
            }
            if (QUERY_INDEX) {
                Duration execTime = Duration.ofMillis(0);
                String query_hint = qe.keyUsed(queryStatement);
                String queryPredicates = queryStatement.getQuery();
                String query_index_query;
                if (query_hint != null) {
                    if (queryStatement.getTemplate() == 3) {
                        queryPredicates = queryPredicates.replace("from PRESENCE", "from PRESENCE force index("
                                + query_hint + ")");
                        query_index_query = "SELECT * from ( " + queryPredicates + " ) as P where " + guardExp.createQueryWithOR();
                    } else
                        query_index_query = "SELECT * from ( SELECT * from PRESENCE force index(" + query_hint
                                + ") where " + queryPredicates + " ) as P where " + guardExp.createQueryWithOR();
                    QueryResult execResult = queryManager.runTimedQueryExp(query_index_query, 1);
                    execTime = execTime.plus(execResult.getTimeTaken());
                    resultString.append(execTime.toMillis()).append(",");
                    System.out.println("Query Index execution : " + " Time: " + execTime.toMillis());
                } else resultString.append("NA").append(","); //No index scan used with query predicate
            }
            if (SIEVE_EXEC) {
                Duration execTime = Duration.ofMillis(0);
                String guardQuery = guardExp.inlineOrNot(true);
                String query_hint = qe.keyUsed(queryStatement);
                String sieve_query;
                /** Calibration of choosing between IndexGuards and IndexQuery
                 *  based on the ratio of querySel/guardTotalCard. In template 3
                 *  because of the join, this ratio is a much smaller number.
                 */
                boolean indexGuards = querySel > 0.5 * guardTotalCard;
                if (queryStatement.getTemplate() == 3) {
                    indexGuards = querySel > 0.01 * guardTotalCard;
                }
                if (indexGuards || query_hint == null) { //Use Guards
                    if (queryStatement.getTemplate() == 3) {
                        sieve_query = guardQuery + queryStatement.getQuery().replace("PRESENCE", "polEval");
                    } else
                        sieve_query = guardQuery + "Select * from polEval where " + queryStatement.getQuery();
                    resultString.append("Guard Index").append(",");
                } else { //Use queries
                    if (queryStatement.getTemplate() == 3) {
                        String query_index = queryStatement.getQuery().replace("from PRESENCE", "from PRESENCE force index("
                                + query_hint + ")");
                        sieve_query = "SELECT * from ( " + query_index + " ) as P where " + guardExp.createQueryWithOR();
                    } else
                        sieve_query = "SELECT * from ( SELECT * from PRESENCE force index(" + query_hint
                                + ") where " + queryStatement.getQuery() + " ) as P where " + guardExp.createQueryWithOR();
                    resultString.append("Query Index").append(",");
                }
                QueryResult execResult = queryManager.runTimedQueryExp(sieve_query, NUM_OF_REPS);
                execTime = execTime.plus(execResult.getTimeTaken());
                resultString.append(execTime.toMillis());
                System.out.println("Sieve Query: " + " Time: " + execTime.toMillis());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
//        System.out.println(resultString);
        return resultString.append("\n").toString();
    }

    public String runGE(String querier, QueryStatement queryStatement, GuardExp guardExp) {

        QueryExplainer qe = new QueryExplainer();
        double querySel = qe.estimateSelectivity(queryStatement);
        StringBuilder resultString = new StringBuilder();
        resultString.append(querier).append(",")
//                .append(userProfile(Integer.parseInt(querier))).append(",")
                .append(queryStatement.getTemplate()).append(",");

//        double querySel = qe.estimateSelectivity(queryStatement);
//        resultString.append(querySel).append(",");

        try {
            GuardPersistor guardPersistor = new GuardPersistor();
            if (guardExp.getGuardParts().isEmpty()) return "empty";

            double guardTotalCard = guardExp.getGuardParts().stream().mapToDouble(GuardPart::getCardinality).sum();

            Duration execTime = Duration.ofMillis(0);
            String guardQuery = guardExp.inlineOrNot(true);
            String query_hint = qe.keyUsed(queryStatement);
            String sieve_query;
            /** Calibration of choosing between IndexGuards and IndexQuery
             *  based on the ratio of querySel/guardTotalCard. In template 3
             *  because of the join, this ratio is a much smaller number.
             */
            boolean indexGuards = querySel > 0.5 * guardTotalCard;
            if (queryStatement.getTemplate() == 3) {
                indexGuards = querySel > 0.01 * guardTotalCard;
            }
            if (indexGuards || query_hint == null) { //Use Guards
                if (queryStatement.getTemplate() == 3) {
                    sieve_query = guardQuery + queryStatement.getQuery().replace("PRESENCE", "polEval");
                } else
                    sieve_query = guardQuery + "Select * from polEval where " + queryStatement.getQuery();
                resultString.append("Guard Index").append(",");
            } else { //Use queries
                if (queryStatement.getTemplate() == 3) {
                    String query_index = queryStatement.getQuery().replace("from PRESENCE", "from PRESENCE force index("
                            + query_hint + ")");
                    sieve_query = "SELECT * from ( " + query_index + " ) as P where " + guardExp.createQueryWithOR();
                } else
                    sieve_query = "SELECT * from ( SELECT * from PRESENCE force index(" + query_hint
                            + ") where " + queryStatement.getQuery() + " ) as P where " + guardExp.createQueryWithOR();
                resultString.append("Query Index").append(",");
            }
            System.out.println();
            QueryResult execResult = queryManager.runTimedQueryExp(sieve_query, NUM_OF_REPS);
            execTime = execTime.plus(execResult.getTimeTaken());
            resultString.append(execTime.toMillis());
            System.out.println("Sieve Query: " + " Time: " + execTime.toMillis());

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(resultString);
        return resultString.append("\n").toString();
    }

    public List<QueryStatement> getQueries(int template, int query_count) {
        WiFiDataSetQueryGeneration qg = new WiFiDataSetQueryGeneration();
        List<QueryStatement> queries = new ArrayList<>();
        queries.addAll(qg.retrieveQueries(template, "all", query_count));
        return queries;
    }

    private String userProfile(int querier) {
        List<Integer> faculty = new ArrayList<>(Arrays.asList(1023, 5352, 11043, 13353, 18575));
        List<Integer> undergrad = new ArrayList<>(Arrays.asList(4686, 7632, 12555, 15936, 15007));
        List<Integer> grad = new ArrayList<>(Arrays.asList(100, 532, 5990, 11815, 32467));
        List<Integer> staff = new ArrayList<>(Arrays.asList(888, 2550, 5293, 9733, 20021));
        if (faculty.contains(querier)) return "faculty";
        else if (undergrad.contains(querier)) return "undergrad";
        else if (grad.contains(querier)) return "graduate";
        else return "staff";
    }

//    public void runExperiment() {
//        QueryPerformance e = new QueryPerformance();
//        PolicyUtil pg = new PolicyUtil();
////        List<Integer> users = pg.getAllUsers(true);
//        //users with increasing number of guards
////        List <Integer> users = new ArrayList<>(Arrays.asList(26389, 15230, 30769, 12445, 36430, 21951,
////                13411, 7079, 364, 26000, 5949, 34372, 6371, 26083, 34290, 2917, 33425, 35503, 26927, 15007));
//        //users with guards of increasing cardinality
////        List <Integer> users = new ArrayList<>(Arrays.asList(14215, 56, 2050, 2819, 37, 625, 23519, 8817, 6215, 387,
////                945, 8962, 23416, 34035));
//        //users with increasing number of policies
////        List <Integer> faculty = new ArrayList<>(Arrays.asList(1023, 5352, 11043, 13353, 18575));
////        List <Integer> undergrad = new ArrayList<>(Arrays.asList(4686, 7632, 12555, 15936, 15007));
////        List<Integer> grad = new ArrayList<>(Arrays.asList(100, 532, 5990, 11815, 32467));
////        List<Integer> staff = new ArrayList<>(Arrays.asList(38,43,53));
//        List<Integer> faculty = new ArrayList<>(Arrays.asList(80,87));
//        List<Integer> users = new ArrayList<>();
//        users.addAll(faculty);
////        users.addAll(undergrad);
////        users.addAll(grad);
////        users.addAll(staff);
//        PolicyPersistor polper = PolicyPersistor.getInstance();
//        String fileName = "query.csv";
////        String file_header = "Querier,Querier_Profile,Query_Type,Query_Cardinality,Number_Of_Policies,Estimated_QPS,Query_Alone," +
////                "Baseline_Policies, Baseline_UDF,Number_of_Guards,Total_Guard_Cardinality,With_Guard_Index,With_Query_Index,Sieve_Parameters, Sieve\n";
//        String file_header = "Querier,Querier_Profile,Query_Type,Query_Cardinality,Number_Of_Policies,Estimated_QPS,Query_Alone," +
//                "Baseline_Policies, Baseline_UDF,Baseline_Index,Number_of_Guards,Total_Guard_Cardinality,Sieve_Parameters, Sieve\n";
//        Writer writer = new Writer();
//        writer.writeString(file_header, PolicyConstants.EXP_RESULTS_DIR, fileName);
//        List<QueryStatement> queries = e.getQueries(3, 12);
//        for (int j = 0; j < queries.size(); j++) {
//            System.out.println("Total Query Selectivity " + queries.get(j).getSelectivity());
//            for (int i = 0; i < users.size(); i++) {
//                String querier = String.valueOf(users.get(i));
//                List<BEPolicy> allowPolicies = polper.retrievePolicies(querier,
//                        PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
//                if (allowPolicies == null) continue;
//                System.out.println("Querier " + querier);
//                writer.writeString(e.runBEPolicies(querier, queries.get(j),
//                        allowPolicies), PolicyConstants.EXP_RESULTS_DIR, fileName);
//                QUERY_EXEC = false;
//            }
//            QUERY_EXEC = true;
//        }
//    }

    public List<CUserGen.User> retrieveQuerier() {
        List<CUserGen.User> users = new ArrayList<>();
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT id, user_id, user_profile, user_group FROM sieve.APP_USER " +
                    "WHERE user_profile IN ('faculty') and " +
                    "user_group NOT IN ('3143-clwa-3019', '3146-clwa-6122', '3143-clwa-3065', '3146-clwa-6219')");
            int count = 0;
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String userId = resultSet.getString("user_id");
                String userProfile = resultSet.getString("user_profile");
                String userGroup = resultSet.getString("user_group");
                CUserGen.User user = new CUserGen.User(id, userId, userProfile, userGroup);
                users.add(user);
                count++;
//                System.out.println("Entry #" + count + ": " + user);
            }
//            System.out.println("Total number of entries: " + count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return users;
    }

    public String runExperiment(QueryStatement query) {
        QueryPerformance e = new QueryPerformance();

        List<CUserGen.User> faculties = e.retrieveQuerier();

        List<Integer> users = new ArrayList<>();

        for (CUserGen.User faculty : faculties) {
            users.add(faculty.getId());
        }

        Random random = new Random();

        for (int i = 0; i < users.size(); i++) {
            int randomIndex = random.nextInt(users.size());
            String querier = String.valueOf(users.get(randomIndex));
            List<BEPolicy> allowPolicies = polper.retrievePolicies(querier,
                    PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
            if (allowPolicies == null) continue;
            else {
                System.out.println("Querier " + querier);
                return querier;
            }
        }
        return "1081";
    }
}