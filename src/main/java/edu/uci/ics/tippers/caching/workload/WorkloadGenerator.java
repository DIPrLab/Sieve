package edu.uci.ics.tippers.caching.workload;

import com.google.gson.Gson;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.generation.query.WiFiDataSet.WiFiDataSetQueryGeneration;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.*;
import edu.uci.ics.tippers.caching.workload.*;
import edu.uci.ics.tippers.model.query.QueryStatement;

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
        int currentTime = 0;
        int nextRegularPolicyInsertionTime = regularInterval;
        int nextDynamicPolicyInsertionTime = dynamicInterval;

// Creating a Gson object for JSON serialization
        Gson gson = new Gson();

// Name of the output JSON file
        String fileName = "workload.json";

// Initializing counters for policies and queries
        int countp = 0; // Counter for policies (not used in this code)
        int countq = 0; // Counter for queries (not used in this code)

// Boolean flag to track if it's the first entry being written to the file
        boolean first = true;

// Try-with-resources block to ensure proper resource management
        try (FileWriter writer = new FileWriter(PolicyConstants.EXP_RESULTS_DIR + fileName)) {
            // Starting the JSON structure with an array of policies
            writer.write("{\"policies\": [");

            // Loop until either policies or queries list becomes empty
            while (!policies.isEmpty() && !queries.isEmpty()) {

                // Writing regular policies if it's time
                if (currentTime >= nextRegularPolicyInsertionTime) {
                    // Generate regular policies and write them to file
                    List<BEPolicy> regularPolicies = extractPolicies(policies, n);
                    for (BEPolicy policy : regularPolicies) {
                        writer.write(gson.toJson(policy) + ","); // Serializing policy object to JSON and writing to file
                    }
                    nextRegularPolicyInsertionTime += regularInterval; // Updating the next insertion time
                }

                // Writing dynamic policies if it's time
                if (currentTime >= nextDynamicPolicyInsertionTime) {
                    // Generate dynamic policies and write them to file
                    Random random = new Random();
                    int noOfPolices = random.nextInt(2 * n) + 1; // Randomly determining the number of dynamic policies
                    List<BEPolicy> dynamicPolicies = extractPolicies(policies, noOfPolices);
                    for (BEPolicy policy : dynamicPolicies) {
                        writer.write(gson.toJson(policy) + ","); // Serializing policy object to JSON and writing to file
                    }
                    nextDynamicPolicyInsertionTime += dynamicInterval; // Updating the next insertion time
                }

                // Generating queries and writing them to file
                List<QueryStatement> generatedQueries = generateQueries(queries, n);
                for (QueryStatement query : generatedQueries) {
                    writer.write(gson.toJson(query) + ","); // Serializing query object to JSON and writing to file
                }

                // Incrementing the current time for the next iteration
                currentTime++;
            }

            // Closing the JSON array of policies and completing the JSON structure
            writer.write("]}");
        } catch (IOException e) {
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

        for (BEPolicy policy : policies) {
            System.out.println(policy.toString());
        }
        System.out.println();

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
    }
}
