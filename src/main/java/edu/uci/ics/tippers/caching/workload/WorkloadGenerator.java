package edu.uci.ics.tippers.caching.workload;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.generation.query.WiFiDataSet.WiFiDataSetQueryGeneration;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.*;
import edu.uci.ics.tippers.caching.workload.*;
import edu.uci.ics.tippers.model.query.QueryStatement;
import edu.uci.ics.tippers.persistor.PolicyPersistor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class WorkloadGenerator {
    private int regularInterval;
    private int dynamicInterval;

    public WorkloadGenerator(int regularInterval, int dynamicInterval) {
        this.regularInterval = regularInterval;
        this.dynamicInterval = dynamicInterval;
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

       /* // Creating a Gson object for JSON serialization
        Gson gson = new Gson();

      //Name of the output JSON file
        String fileName = "workload.json";

        // Create a JSON object to hold the data
        JsonObject workloadJson = new JsonObject();

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

        // Create an array to hold policies and queries
        JsonArray policiesAndQueriesArray = new JsonArray();

        // Add policies to the array
        for (BEPolicy policy : policies) {
            JsonObject policyJson = gson.toJsonTree(policy).getAsJsonObject();
            policyJson.addProperty("type", "BEPolicy");
            policiesAndQueriesArray.add(policyJson);
        }

        // Add queries to the array
        for (QueryStatement query : queries) {
            JsonObject queryJson = gson.toJsonTree(query).getAsJsonObject();
            queryJson.addProperty("type", "QueryStatement");
            policiesAndQueriesArray.add(queryJson);
        }

        // Add the policies and queries array to the workload JSON object
        workloadJson.add("Policies_and_Queries", policiesAndQueriesArray);

        // Write the JSON object to a file
        try (FileWriter writer = new FileWriter(PolicyConstants.EXP_RESULTS_DIR + fileName)) {
            gson.toJson(workloadJson, writer);
        } catch (IOException e) {
            e.printStackTrace();
        }*/
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
//
    private List<QueryStatement> generateQueries(List<QueryStatement> queries, int n) {
        // Query generation logic here
        List<QueryStatement> extractedQueries = new ArrayList<>();
        for (int i = 0; i < n && !queries.isEmpty(); i++) {
            extractedQueries.add(queries.remove(0)); // Remove and add the first policy from the list
        }
        return extractedQueries;
    }

    public void runExperiment() {
        // generating policies
        CPolicyGen cpg = new CPolicyGen();
        CUserGen cUserGen = new CUserGen();
        List<CUserGen.User> users = cUserGen.retrieveUserData();

        List<BEPolicy> policies = cpg.generatePolicies(users);

//        for (BEPolicy policy : policies) {
//            System.out.println(policy.toString());
//        }
//        System.out.println();

        CQueryGen cqg = new CQueryGen();
        boolean[] templates = {true, true, true, true};
        int numOfQueries = 4;
        List<QueryStatement> queries = cqg.constructWorkload(templates, numOfQueries);
        for (QueryStatement query : queries) {
            System.out.println(query.toString());
        }
        System.out.println();

        int regularInterval = 4; // Example regular interval
        int dynamicInterval = 2; // Example dynamic interval
        WorkloadGenerator generator = new WorkloadGenerator(regularInterval, dynamicInterval);
        int numPoliciesQueries = 1; // Example number of policies/queries to generate each interval
        generator.generateWorkload(numPoliciesQueries, policies, queries);

        //testing json BEpoilices to database
        generator.persistBEPoliciesFromJsonToDatabase();

    }


}
