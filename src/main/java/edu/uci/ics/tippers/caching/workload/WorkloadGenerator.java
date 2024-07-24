package edu.uci.ics.tippers.caching.workload;

import edu.uci.ics.tippers.caching.CachingAlgorithm;
import edu.uci.ics.tippers.caching.CircularHashMap;
import edu.uci.ics.tippers.caching.ClockHashMap;
import edu.uci.ics.tippers.caching.costmodel.CMWorkoad;
import edu.uci.ics.tippers.caching.costmodel.CostModelsExp;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
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
import java.time.Duration;
import java.time.Instant;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
    CostModelsExp cme;
    ClockHashMap<String, GuardExp> clockMap;

    public WorkloadGenerator(int regularInterval, int dynamicIntervalStart, int duration) {
        this.regularInterval = regularInterval;
        this.dynamicIntervalStart = dynamicIntervalStart;
        this.duration = duration;
        polper = PolicyPersistor.getInstance();
        e = new QueryPerformance();
        ca = new CachingAlgorithm();
        cme = new CostModelsExp();
        clockMap = new ClockHashMap<>();
    }

    public WorkloadGenerator(int regularInterval) {
        this.regularInterval = regularInterval;
        this.dynamicIntervalStart = 0;
        this.duration = 0;
        polper = PolicyPersistor.getInstance();
        e = new QueryPerformance();
        ca = new CachingAlgorithm();
        cme = new CostModelsExp();
        clockMap = new ClockHashMap<>(3);
    }

    public void generateWorkload(int n, List<BEPolicy> policies, List<QueryStatement> queries) {
        //only Persisting BEPolicy to json file
        // Create an ObjectMapper instance for JSON serialization
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            objectMapper.writeValue(new File(PolicyConstants.EXP_RESULTS_DIR + "policies.json"), policies);
            System.out.println("***** JSON File Successfully Written ***** ");
        } catch (IOException e) {
            System.out.println("***** ERROR ********** JSON File Not Written ***** ");
            e.printStackTrace();
        }
    public Duration generateWorkload(int n, List<BEPolicy> policies, List<QueryStatement> queries) {
        int currentTime = 0;
        int nextRegularPolicyInsertionTime = 0;
        int sizeOfPolicies = policies.size();
        int dynamicPolicySize = (int) Math.floor(sizeOfPolicies/3);
        int windowSize = 10;
        int generatedQueries = 0;
        boolean cachingFlag = true;
        LinkedList<QueryStatement> queryWindow = new LinkedList<>();
//        System.out.println(dynamicPolicySize);

        QueryStatement query = new QueryStatement();
        Random random = new Random();
        int batchSize = 2;
        List<QueryStatement> batchQueries = new ArrayList<>();

        CircularHashMap<String,Timestamp> timestampDirectory = new CircularHashMap<>(400);
        ClockHashMap<String, GuardExp> clockHashMap = new ClockHashMap<>(400);

        Writer writer = new Writer();
        StringBuilder result = new StringBuilder();
        String fileName = "gcp_M_S40P1Q.txt";

        boolean first = true;

        result.append("No. of policies= "). append(policies.size()).append("\n")
                .append("No. of queries= ").append(queries.size()).append("\n")
                .append("Interleaving Techniques= ").append("[Constant Interval= ").append(regularInterval).append("]")
                .append("[Variable Interval= ").append(dynamicIntervalStart).append(",").append(dynamicIntervalStart+duration).append("]").append("\n");
        writer.writeString(result.toString(), PolicyConstants.EXP_RESULTS_DIR, fileName);

        Instant fsStart = Instant.now();
       /* // Creating a Gson object for JSON serialization
        Gson gson = new Gson();

//        while (!policies.isEmpty() && !queries.isEmpty()) {
      //Name of the output JSON file
        String fileName = "workload.json";

        if(cachingFlag){
            System.out.println("!!!Caching+Merge!!!");
            while (!queries.isEmpty() && !policies.isEmpty()) {
                if (currentTime == 0 || currentTime == nextRegularPolicyInsertionTime) {
                    List<BEPolicy> regularPolicies = extractPolicies(policies, n);
        // Create a JSON object to hold the data
        JsonObject workloadJson = new JsonObject();

                    //Insert policy into database
                    for(BEPolicy policy: regularPolicies){
                        result.append(currentTime).append(",")
                                .append(policy.toString()).append("\n");
                        Instant pinsert = Instant.now();
                        Timestamp policyinsertionTime = Timestamp.from(pinsert);
                        timestampDirectory.put(policy.fetchQuerier(),policyinsertionTime);
                        policy.setInserted_at(policyinsertionTime);
                    }
                    nextRegularPolicyInsertionTime += regularInterval;

                    polper.insertPolicy(regularPolicies);
                }


                // Bursty State
/*                if(generatedQueries <=4000){
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
//                    ca.runAlgorithm(clockHashMap, querier, query, timestampDirectory);
                }else{*/
//                    int queriesToGenerate = Math.min(batchSize, queries.size());
//                    for(int i = 0; i < queriesToGenerate; i++){
//                        if (generatedQueries % 2 == 0){
//                            if(queryWindow.size() < windowSize){
//                                queryWindow.add(queries.remove(0));
//                            }else{
//                                queryWindow.removeFirst();
//                                queryWindow.add(queries.remove(0));
//                            }
//                            query = queryWindow.getLast();
//                        }else{
//                            int index = random.nextInt(queryWindow.size());
//                            query = queryWindow.get(index);
//                        }
//                        generatedQueries++;
//                        batchQueries.add(query);
//                    }
//
//                    if (!batchQueries.isEmpty()) {
//                        for (QueryStatement q : batchQueries) {
//                            result.append(currentTime).append(",")
//                                    .append(q.toString()).append("\n");
//                            String querier = e.runExperiment(q);
//                            ca.runAlgorithm(clockHashMap, querier, query, timestampDirectory);
//                        }
//                        batchQueries.clear(); // Clear the batch list for the next set of queries
//                    }
//                }

//                Steady State
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
               ca.runAlgorithm(clockHashMap, querier, query, timestampDirectory);
//                cme.runAlgorithm(clockHashMap, querier, query, timestampDirectory);
        // Add the number of policies and queries
        workloadJson.addProperty("No_of_policies", policies.size());
        workloadJson.addProperty("No_of_queries", queries.size());

        // Add interleaving techniques
        JsonObject interleavingTechniques = new JsonObject();
        interleavingTechniques.addProperty("Constant_Interval", regularInterval);
        JsonArray variableIntervalArray = new JsonArray();
        variableIntervalArray.add(0);
        variableIntervalArray.add(dynamicInterval);
        interleavingTechniques.add("Variable_Interval", variableIntervalArray);
        workloadJson.add("Interleaving_Techniques", interleavingTechniques);


                // Writing results to file
                if (!first) writer.writeString(result.toString(), PolicyConstants.EXP_RESULTS_DIR, fileName);
                else first = false;

                // Clearing StringBuilder for the next iteration
                result.setLength(0);

                currentTime++;

            }
        }else{
            System.out.println("!!! Without Caching!!!");
            while (!policies.isEmpty() && !queries.isEmpty()) {
//                if (generatedQueries <= 2000) n = 10;
//                else if (generatedQueries <= 4000) n = 5;
//                else n = 1;

                if (currentTime == 0 || currentTime == nextRegularPolicyInsertionTime) {
                    // Generate regular policies and write them to file
                    List<BEPolicy> regularPolicies = extractPolicies(policies, n);

                    //Insert policy into database
                    for(BEPolicy policy: regularPolicies){
                        result.append(currentTime).append(",")
                                .append(policy.toString()).append("\n");
                        Instant pinsert = Instant.now();
                        Timestamp policyinsertionTime = Timestamp.from(pinsert);
                        policy.setInserted_at(policyinsertionTime);
                    }
                    nextRegularPolicyInsertionTime += regularInterval;
        // Create an array to hold policies and queries
        JsonArray policiesAndQueriesArray = new JsonArray();

                    polper.insertPolicy(regularPolicies);
                }
        // Add policies to the array
        for (BEPolicy policy : policies) {
            JsonObject policyJson = gson.toJsonTree(policy).getAsJsonObject();
            policyJson.addProperty("type", "BEPolicy");
            policiesAndQueriesArray.add(policyJson);
        }


//                // Bursty State
//                if(generatedQueries <=4000){
//                    if (generatedQueries % 2 == 0){
//                        if(queryWindow.size() < windowSize){
//                            queryWindow.add(queries.remove(0));
//                        }else{
//                            queryWindow.removeFirst();
//                            queryWindow.add(queries.remove(0));
//                        }
//                        query = queryWindow.getLast();
//                    }else{
//                        int index = random.nextInt(queryWindow.size());
//                        query = queryWindow.get(index);
//                    }
//                    generatedQueries++;
//                    result.append(currentTime).append(",")
//                            .append(query.toString()).append("\n");
//                    String querier = e.runExperiment(query);
//                    ca.runAlgorithm(clockHashMap, querier, query, timestampDirectory);
//                }else{
//                    int queriesToGenerate = Math.min(batchSize, queries.size());
//                    for(int i = 0; i < queriesToGenerate; i++){
//                        if (generatedQueries % 2 == 0){
//                            if(queryWindow.size() < windowSize){
//                                queryWindow.add(queries.remove(0));
//                            }else{
//                                queryWindow.removeFirst();
//                                queryWindow.add(queries.remove(0));
//                            }
//                            query = queryWindow.getLast();
//                        }else{
//                            int index = random.nextInt(queryWindow.size());
//                            query = queryWindow.get(index);
//                        }
//                        generatedQueries++;
//                        batchQueries.add(query);
//                    }
//
//                    if (!batchQueries.isEmpty()) {
//                        for (QueryStatement q : batchQueries) {
//                            result.append(currentTime).append(",")
//                                    .append(q.toString()).append("\n");
//                            String querier = e.runExperiment(q);
//                            ca.runAlgorithm(clockHashMap, querier, q, timestampDirectory);
//                        }
//                        batchQueries.clear(); // Clear the batch list for the next set of queries
//                    }
//                }

//                Steady State
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
                GuardExp GE = ca.SieveGG(querier, query);
                String answer = e.runGE(querier, query, GE);
//                System.out.println(GE);
        // Add queries to the array
        for (QueryStatement query : queries) {
            JsonObject queryJson = gson.toJsonTree(query).getAsJsonObject();
            queryJson.addProperty("type", "QueryStatement");
            policiesAndQueriesArray.add(queryJson);
        }

                // Writing results to file
                if (!first) writer.writeString(result.toString(), PolicyConstants.EXP_RESULTS_DIR, fileName);
                else first = false;
        // Add the policies and queries array to the workload JSON object
        workloadJson.add("Policies_and_Queries", policiesAndQueriesArray);

                // Clearing StringBuilder for the next iteration
                result.setLength(0);

                currentTime++;
        // Write the JSON object to a file
        try (FileWriter writer = new FileWriter(PolicyConstants.EXP_RESULTS_DIR + fileName)) {
            gson.toJson(workloadJson, writer);
        } catch (IOException e) {
            e.printStackTrace();
        }*/
    }

            }
        }
    private static List<BEPolicy> readBEPolicyFromJson(ObjectMapper objectMapper) {
        List<BEPolicy> localPolicies = new ArrayList<>();
        try {
            localPolicies = objectMapper.readValue(new File(PolicyConstants.EXP_RESULTS_DIR + "policies.json"), new TypeReference<List<BEPolicy>>() {});
        } catch (IOException e) {
            System.out.println("***** Error Parsing json BEPolicies ***** ");
            e.printStackTrace();
        }
        return localPolicies;
    }

        Instant fsEnd = Instant.now();
        Duration totalRunTime = Duration.between(fsStart, fsEnd);
        return totalRunTime;
    }

    private void persistBEPoliciesFromJsonToDatabase() {
        ObjectMapper objectMapper = new ObjectMapper();
        List<BEPolicy> localPolicies = readBEPolicyFromJson(objectMapper);
        System.out.println("JsonPolicies count: " + localPolicies.size());
        PolicyPersistor polper = PolicyPersistor.getInstance();
        try {
            polper.insertPolicy(localPolicies);
            System.out.println("***** Successfully inserted JSON policies into the database. ***** ");
        } catch (Exception e) {
            System.out.println("***** Failed to insert JSON policies into the database. ***** ");
            e.printStackTrace();
        }
    }
    private List<BEPolicy> extractPolicies(List<BEPolicy> policies, int n) {
        List<BEPolicy> extractedPolicies = new ArrayList<>();
        for (int i = 0; i < n && !policies.isEmpty(); i++) {
            extractedPolicies.add(policies.remove(0)); // Remove and add the first policy from the list
        }
        return extractedPolicies;
    }

    public void runExperiment() {
        // generating policies

        CUserGen cUserGen = new CUserGen();
        List<CUserGen.User> users = cUserGen.retrieveUserData();

        CPolicyGen cpg = new CPolicyGen();
        List<BEPolicy> policies = cpg.generatePolicies(users);

        System.out.println("Total number of entries: " + users.size());
        System.out.println("Total number of entries: " + policies.size());
//        for (BEPolicy policy : policies) {
//            System.out.println(policy.toString());
//        }
//        System.out.println();

        int queryCount = 3940;
        boolean[] templates = {true, true, false, false};
        List<QueryStatement> queries = new ArrayList<>();
        for (int i = 0; i < templates.length; i++) {
            if (templates[i]) queries.addAll(e.getQueries(i+1,queryCount));
//            for (QueryStatement query : queries) {
//                System.out.println(query.toString());
//            }
//            System.out.println();
        }

        System.out.println("Total number of entries: " + users.size());
        System.out.println("Total number of policies: " + policies.size());
        System.out.println("Total number of queries: " + queries.size());

        int regularInterval = 1; // Example regular interval
//        int dynamicInterval = 1; // Example dynamic interval
//        int duration = 3;

        WorkloadGenerator generator = new WorkloadGenerator(regularInterval);
//        WorkloadGenerator generator = new WorkloadGenerator(regularInterval, dynamicInterval, duration);

        int numPoliciesQueries = 5; // Example number of policies/queries to generate each interval
        Duration totalRunTime = generator.generateWorkload(numPoliciesQueries, policies, queries);
        System.out.println("Total Run Time: " + totalRunTime);
        int regularInterval = 4; // Example regular interval
        int dynamicInterval = 2; // Example dynamic interval
        WorkloadGenerator generator = new WorkloadGenerator(regularInterval, dynamicInterval);
        int numPoliciesQueries = 1; // Example number of policies/queries to generate each interval
        generator.generateWorkload(numPoliciesQueries, policies, queries);

        //testing json BEpoilices to database
        generator.persistBEPoliciesFromJsonToDatabase();

    }


}
