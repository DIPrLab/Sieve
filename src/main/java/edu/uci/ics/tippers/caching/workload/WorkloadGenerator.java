package edu.uci.ics.tippers.caching.workload;

import edu.uci.ics.tippers.caching.CachingAlgorithm;
import edu.uci.ics.tippers.caching.ClockHashMap;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.execution.experiments.performance.QueryPerformance;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.generation.query.WiFiDataSet.WiFiDataSetQueryGeneration;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.*;
import edu.uci.ics.tippers.caching.workload.*;
import edu.uci.ics.tippers.model.query.QueryStatement;
import edu.uci.ics.tippers.persistor.PolicyPersistor;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class WorkloadGenerator {
    private int regularInterval;
    private int dynamicIntervalStart;
    private int duration;
    PolicyPersistor polper;
    QueryPerformance e;
    CachingAlgorithm ca;
    ClockHashMap<String, GuardExp> clockMap;

    public WorkloadGenerator(int regularInterval, int dynamicIntervalStart, int duration) {
        this.regularInterval = regularInterval;
        this.dynamicIntervalStart = dynamicIntervalStart;
        this.duration = duration;
        polper = PolicyPersistor.getInstance();
        e = new QueryPerformance();
        ca = new CachingAlgorithm();
        clockMap = new ClockHashMap<>();
    }

    public WorkloadGenerator(int regularInterval) {
        this.regularInterval = regularInterval;
        this.dynamicIntervalStart = 0;
        this.duration = 0;
        polper = PolicyPersistor.getInstance();
        e = new QueryPerformance();
        ca = new CachingAlgorithm();
        clockMap = new ClockHashMap<>(3);
    }

    public void generateWorkload(int n, List<BEPolicy> policies, List<QueryStatement> queries) {
        int currentTime = 0;
        int nextRegularPolicyInsertionTime = 0;
        int sizeOfPolicies = policies.size();
        int dynamicPolicySize = (int) Math.floor(sizeOfPolicies/3);
        int windowSize = 10;
        int generatedQueries = 0;
        LinkedList<QueryStatement> queryWindow = new LinkedList<>();
//        System.out.println(dynamicPolicySize);

        long millis = Instant.now().toEpochMilli();
        Timestamp timestampGlobal = new Timestamp(millis);
        ClockHashMap<String, GuardExp> clockHashMap = new ClockHashMap<>((int)Math.floor(queries.size() * 0.1));

        Writer writer = new Writer();
        StringBuilder result = new StringBuilder();
        String fileName = "workload_sample.txt";

        boolean first = true;

        result.append("No. of policies= "). append(policies.size()).append("\n")
                .append("No. of queries= ").append(queries.size()).append("\n")
                .append("Interleaving Techniques= ").append("[Constant Interval= ").append(regularInterval).append("]")
                .append("[Variable Interval= ").append(dynamicIntervalStart).append(",").append(dynamicIntervalStart+duration).append("]").append("\n");
        writer.writeString(result.toString(), PolicyConstants.EXP_RESULTS_DIR, fileName);


        while (!policies.isEmpty() && !queries.isEmpty()) {


            /*if (currentTime >= dynamicIntervalStart && currentTime <= (dynamicIntervalStart+duration)) {
                // Generate dynamic policies and write them to file

                while (dynamicPolicySize > 0){
                    Random random = new Random();
                    int noOfPolices = random.nextInt(dynamicPolicySize)+1;
                    List<BEPolicy> dynamicPolicies = extractPolicies(policies, noOfPolices);
                    for(BEPolicy policy: dynamicPolicies){
                        result.append(currentTime).append(",")
                                .append(policy.toString()).append("\n");
                    }

                    polper.insertPolicy(dynamicPolicies);
                    dynamicPolicySize -= noOfPolices;
                }
            } else*/ if (currentTime == 0 || currentTime == nextRegularPolicyInsertionTime) {
                // Generate regular policies and write them to file
//                if (currentTime != dynamicIntervalStart && currentTime >= (dynamicIntervalStart+duration)){
                    List<BEPolicy> regularPolicies = extractPolicies(policies, n);

                    //Insert policy into database
                    for(BEPolicy policy: regularPolicies){
                        result.append(currentTime).append(",")
                                .append(policy.toString()).append("\n");
                    }

                    nextRegularPolicyInsertionTime += regularInterval;

                    polper.insertPolicy(regularPolicies);
//                }
            }

            // Generate queries and write them to file
            // Sliding window to store last 10 queries
            QueryStatement query = new QueryStatement();
            Random random = new Random();
            if (generatedQueries % 2 == 0){
                if(queryWindow.size() < windowSize){
                    queryWindow.add(queries.remove(0));
                }else{
                    queryWindow.removeFirst();
                    queryWindow.add(queries.remove(0));
                }
                query = queryWindow.getLast();
            }else{
                int index = random.nextInt(queryWindow.size());
                query = queryWindow.get(index);
            }
            generatedQueries++;
            result.append(currentTime).append(",")
                    .append(query.toString()).append("\n");
            String querier = e.runExperiment(query);
            ca.runAlgorithm(clockHashMap, querier, query, timestampGlobal);


            // Writing results to file
            if (!first) writer.writeString(result.toString(), PolicyConstants.EXP_RESULTS_DIR, fileName);
            else first = false;

            // Clearing StringBuilder for the next iteration
            result.setLength(0);

            currentTime++;

        }
    }

    private List<BEPolicy> extractPolicies(List<BEPolicy> policies, int n) {
        List<BEPolicy> extractedPolicies = new ArrayList<>();
        for (int i = 0; i < n && !policies.isEmpty(); i++) {
            extractedPolicies.add(policies.remove(0)); // Remove and add the first policy from the list
        }
        return extractedPolicies;
    }
//
    private List<QueryStatement> generateQueries(List<QueryStatement> queries, int queriesGenerated) {
        // Query generation logic here
        List<QueryStatement> extractedQueries = new ArrayList<>();
        LinkedList<QueryStatement> queryWindow = new LinkedList<>(); // Sliding window to store last 10 queries

        Random random = new Random();
        int totalQueries = queries.size();



        return extractedQueries;
    }

    public void runExperiment() {
        // generating policies

        CUserGen cUserGen = new CUserGen();
        List<CUserGen.User> users = cUserGen.retrieveUserData();

        CPolicyGen cpg = new CPolicyGen();
        List<BEPolicy> policies = cpg.generatePolicies(users);

        System.out.println("Total number of entries: " + users.size());
        System.out.println("Total number of entries: " + policies.size());

        int queryCount = 552;
        boolean[] templates = {true, true, false, false};
        List<QueryStatement> queries = new ArrayList<>();
        for (int i = 0; i < templates.length; i++) {
            if (templates[i]) queries.addAll(e.getQueries(i+1,queryCount));
            for (QueryStatement query : queries) {
                System.out.println(query.toString());
            }
            System.out.println();
        }

        System.out.println("Total number of entries: " + users.size());
        System.out.println("Total number of policies: " + policies.size());
        System.out.println("Total number of queries: " + queries.size());

        int regularInterval = 1; // Example regular interval
//        int dynamicInterval = 1; // Example dynamic interval
//        int duration = 3;

        WorkloadGenerator generator = new WorkloadGenerator(regularInterval);
//        WorkloadGenerator generator = new WorkloadGenerator(regularInterval, dynamicInterval, duration);

        int numPoliciesQueries = 2; // Example number of policies/queries to generate each interval
        generator.generateWorkload(numPoliciesQueries, policies, queries);
    }
}