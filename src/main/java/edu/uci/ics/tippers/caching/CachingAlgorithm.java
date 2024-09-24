package edu.uci.ics.tippers.caching;

import java.sql.Connection;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.mysql.MySQLConnectionManager;
import edu.uci.ics.tippers.execution.experiments.performance.QueryPerformance;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.generation.policy.WiFiDataSet.PolicyUtil;
import edu.uci.ics.tippers.model.guard.SelectGuard;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.query.QueryStatement;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.persistor.GuardPersistor;
import edu.uci.ics.tippers.persistor.PolicyPersistor;


public class CachingAlgorithm <C,Q> {

    private Connection connection;

    Random r;
    PolicyPersistor polper;
    GuardPersistor guardPersistor;
    PolicyUtil pg;
    QueryPerformance e;
    Writer writer;
    StringBuilder result;
    String fileName;

    public CachingAlgorithm(){
        connection = MySQLConnectionManager.getInstance().getConnection();
        r = new Random();
        polper = PolicyPersistor.getInstance();
        pg = new PolicyUtil();
        this.guardPersistor = new GuardPersistor();
        e = new QueryPerformance();
        writer = new Writer();
        result = new StringBuilder();

        fileName = "CEE_C_S1P2Q_80.csv";
        result.append("Querier"). append(",")
                .append("Cache log").append(",")
                .append("Generation Time").append(",")
                .append("Execution Time").append("\n");
        writer.writeString(result.toString(), PolicyConstants.EXP_RESULTS_DIR, fileName);

    }

    public void runAlgorithm(ClockHashMap<String, GuardExp> clockHashMap, String querier, QueryStatement query, CircularHashMap<String,Timestamp> timestampDirectory) {

        // Implementation of the Guard Caching Algorithm
        int index;

        GuardExp newGE = new GuardExp();

        boolean first = true;

        if(clockHashMap.size == 0){
            index = 0;
        }else{
            index = clockHashMap.getIndex(querier);
        }

        if (index != 0) {
            GuardExp guardExp = clockHashMap.get(querier);
            Timestamp timestampGE = guardExp.getLast_updated();

            Timestamp lastestTimestamp = timestampDirectory.get(querier);
            if (lastestTimestamp != null && lastestTimestamp.before(timestampGE)) {
                clockHashMap.update(querier);

                Instant fsStart = Instant.now();
                Duration totalExeTime = Duration.ofMillis(0);
                String answer = e.runGE(querier, query, guardExp);
                Instant fsEnd = Instant.now();
                totalExeTime = totalExeTime.plus(Duration.between(fsStart, fsEnd));
                double seconds = totalExeTime.getSeconds() + totalExeTime.getNano() / 1_000_000.0;

//                System.out.println(answer);
                result.append(querier).append(",")
                        .append("hit").append(",")
                        .append(0).append(",")
                        .append(seconds).append("\n");
                writer.writeString(result.toString(), PolicyConstants.EXP_RESULTS_DIR, fileName);
                newGE = guardExp;
            }else{
                Instant fsStart = Instant.now();
                Duration totalGenTime = Duration.ofMillis(0);
                newGE = SieveGG(querier, query);
                Instant fsEnd = Instant.now();
                totalGenTime = totalGenTime.plus(Duration.between(fsStart, fsEnd));
                double secondsG = totalGenTime.getSeconds() + totalGenTime.getNano() / 1_000_000.0;

                clockHashMap.put(querier, newGE);

                fsStart = Instant.now();
                Duration totalExeTime = Duration.ofMillis(0);
                String answer = e.runGE(querier, query, newGE);
                fsEnd = Instant.now();
                totalExeTime = totalExeTime.plus(Duration.between(fsStart, fsEnd));
                double secondsE = totalExeTime.getSeconds() + totalExeTime.getNano() / 1_000_000.0;

                result.append(querier).append(",")
                        .append("soft-hit").append(",")
                        .append(secondsG).append(",")
                        .append(secondsE).append("\n");
                writer.writeString(result.toString(), PolicyConstants.EXP_RESULTS_DIR, fileName);

            }
        }else{
        // If querier not found or no matching GE, create a new one
            Instant fsStart = Instant.now();
            Duration guardGen = Duration.ofMillis(0);
            newGE = SieveGG(querier, query);
            Instant fsEnd = Instant.now();
            guardGen = guardGen.plus(Duration.between(fsStart, fsEnd));
            double secondsG = guardGen.getSeconds() + guardGen.getNano() / 1_000_000.0;
            if (newGE == null){
                return;
            }else {
                clockHashMap.put(querier, newGE);
                fsStart = Instant.now();
                Duration totalExeTime = Duration.ofMillis(0);
                String answer = e.runGE(querier, query, newGE);
//                System.out.println(answer);
                fsEnd = Instant.now();
                totalExeTime = totalExeTime.plus(Duration.between(fsStart, fsEnd));
                double secondsE = totalExeTime.getSeconds() + totalExeTime.getNano() / 1_000_000.0;
                result.append(querier).append(",")
                        .append("miss").append(",")
                        .append(secondsG).append(",")
                        .append(secondsE).append("\n");
                writer.writeString(result.toString(), PolicyConstants.EXP_RESULTS_DIR, fileName);
            }
        }

        // Writing results to file
        if (!first) writer.writeString(result.toString(), PolicyConstants.EXP_RESULTS_DIR, fileName);
        else first = false;

        // Clearing StringBuilder for the next iteration
        result.setLength(0);

        return;

    }

    public GuardExp SieveGG (String querier, QueryStatement query){
        QueryPerformance e = new QueryPerformance();
        List<BEPolicy> allowPolicies = polper.retrievePolicies(querier,
                    PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);

        if(allowPolicies == null) return null;
        System.out.println("Querier #: " + querier + " with " + allowPolicies.size() + " allow policies");
        BEExpression allowBeExpression = new BEExpression(allowPolicies);
        Duration guardGen = Duration.ofMillis(0);

        Instant fsStart = Instant.now();
        SelectGuard gh = new SelectGuard(allowBeExpression, true);
        Instant fsEnd = Instant.now();

        System.out.println(gh.createGuardedQuery(true));
        guardGen = guardGen.plus(Duration.between(fsStart, fsEnd));

        System.out.println("Guard Generation time: " + guardGen + " Number of Guards: " + gh.numberOfGuards());

//        guardPersistor.insertGuard(gh.create(String.valueOf(querier), "user"));

//        System.out.println(e.runBEPolicies(querier,query,allowPolicies));

        return gh.create(String.valueOf(querier), "user");
    }
    public List<BEPolicy> fetchNewPolicies(String querier, Timestamp timestampGlobal) {
        // Implementation for fetchNewPolicies function
        List<BEPolicy> allowPolicies = polper.retrievePolicies(querier,
                PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
        List<BEPolicy> newPolicies = null;
        for (int i=0; i< allowPolicies.size(); i++) {
            if (allowPolicies.get(i).getInserted_at().after(timestampGlobal)){
                newPolicies.add(allowPolicies.get(i));
            }
        }
        return newPolicies; // Placeholder, replace with actual implementation
    }

    public static void main(String[] args) {
        // Example usage of CachingAlgorithm
        long millis = Instant.now().toEpochMilli();
        Timestamp timestampGlobal = new Timestamp(millis);
        ClockHashMap<String, GuardExp> clockHashMap = new ClockHashMap<>(3);
        QueryPerformance e = new QueryPerformance();
        List<QueryStatement> queries = e.getQueries(3, 9);
        QueryStatement query = new QueryStatement();
        CachingAlgorithm ca = new CachingAlgorithm();
        String querier = null;
//        ca.runAlgorithm(clockHashMap, querier, query, timestampGlobal);
    }
}
