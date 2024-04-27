package edu.uci.ics.tippers.caching.workload;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.generation.query.WiFiDataSet.WiFiDataSetQueryGeneration;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.*;
import edu.uci.ics.tippers.caching.workload.*;
import edu.uci.ics.tippers.model.query.QueryStatement;

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
        Writer writer = new Writer();
        StringBuilder result = new StringBuilder();
        String fileName = "workload.txt";
        int countp = 0;
        int countq = 0;
        boolean first = true;
        result.append("No. of policies= "). append(policies.size()).append("\n")
                .append("No. of queries= ").append(queries.size()).append("\n")
                .append("Interleaving Techniques= ").append("[Constant Interval= ").append(regularInterval).append("]")
                .append("[Variable Interval= 0,2*").append(dynamicInterval).append("]").append("\n");
        writer.writeString(result.toString(), PolicyConstants.EXP_RESULTS_DIR, fileName);

        while (!policies.isEmpty() && !queries.isEmpty()) {


            if (currentTime >= nextRegularPolicyInsertionTime) {
                // Generate regular policies and write them to file
                List<BEPolicy> regularPolicies = extractPolicies(policies, n);

                for(BEPolicy policy: regularPolicies){
                    result.append(currentTime).append(",")
                            .append(policy.toString()).append("\n");
                }
                nextRegularPolicyInsertionTime += regularInterval;
            }

            if (currentTime >= nextDynamicPolicyInsertionTime) {
                // Generate dynamic policies and write them to file
                Random random = new Random();
                int noOfPolices = random.nextInt(2*n)+1;
                List<BEPolicy> dynamicPolicies = extractPolicies(policies, noOfPolices);
                for(BEPolicy policy: dynamicPolicies){
                    result.append(currentTime).append(",")
                            .append(policy.toString()).append("\n");
                }

                nextDynamicPolicyInsertionTime += dynamicInterval;
            }

            // Generate queries and write them to file
            List<QueryStatement> generatedQueries = generateQueries(queries, n);
            for(QueryStatement query: generatedQueries){
                result.append(currentTime).append(",")
                        .append(query.toString()).append("\n");
            }

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
