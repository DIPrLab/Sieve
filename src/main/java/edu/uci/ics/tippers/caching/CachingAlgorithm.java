//package edu.uci.ics.tippers.caching;
//
//import java.sql.Connection;
//import java.sql.Timestamp;
//import java.time.Duration;
//import java.time.Instant;
//import java.util.*;
//
//import edu.uci.ics.tippers.common.PolicyConstants;
//import edu.uci.ics.tippers.dbms.mysql.MySQLConnectionManager;
//import edu.uci.ics.tippers.execution.experiments.performance.QueryPerformance;
//import edu.uci.ics.tippers.generation.policy.WiFiDataSet.PolicyUtil;
//import edu.uci.ics.tippers.model.guard.SelectGuard;
//import edu.uci.ics.tippers.model.policy.BEExpression;
//import edu.uci.ics.tippers.model.policy.BEPolicy;
//import edu.uci.ics.tippers.model.query.QueryStatement;
//import edu.uci.ics.tippers.model.guard.GuardExp;
//import edu.uci.ics.tippers.persistor.GuardPersistor;
//import edu.uci.ics.tippers.persistor.PolicyPersistor;
//
//
//public class CachingAlgorithm <C,Q> {
//
//    private Connection connection;
//
//    Random r;
//    PolicyPersistor polper;
//    GuardPersistor guardPersistor;
//    PolicyUtil pg;
//
//    public CachingAlgorithm(){
//        connection = MySQLConnectionManager.getInstance().getConnection();
//        r = new Random();
//        polper = PolicyPersistor.getInstance();
//        pg = new PolicyUtil();
//
//    }
//
//    public void runAlgorithm(ClockHashMap<Integer, List<GuardExp>> clockHashMap, QueryStatement query, Timestamp timestampGlobal) {
//
//        // Implementation of the Guard Caching Algorithm
//        Integer querier = query.getId();
//        int index = clockHashMap.getIndex(querier);
//        List<BEPolicy> newPolicies = null;
//
//        if (index != 0) {
//            List<GuardExp> listGE = (List<GuardExp>) clockHashMap.get(querier);
//            Timestamp timestamp;
//            for(int i=0; i< listGE.size(); i++){
//                if(timestamp.after(listGE.get(i).getLast_updated())) {
//                   // to do;//// verify
//                }
//            }
//            Timestamp timestamp = ge.getLast_updated();
//
//            boolean t = timestampGlobal.equals(timestamp);
//            if (t) {
//                clockHashMap.update(querier);
//                return;
//            } else {
//                // Fetch policies that have timestamp greater than the GE stored
//                newPolicies = fetchNewPolicies(querier, timestampGlobal);
//
//                // Implement CASE 1 and CASE 2
//                // costMethod1 and costMethod2 calculation
//                int cost1 = costMethod1();
//                int cost2 = costMethod2();
//
//                if (cost1 > cost2) {
//                    concatenatePNew(PoliciesInGE_i, P_new); // create a new ge which includes all the policies???
//                    clockHashMap.findAndUpdate(querier, listGE);
//                    return;
//                } else {
//                    // Implement GE_i union with GE_new
//                    GuardExp newGE = SieveGG(querier, newPolicies);
//
//                    clockHashMap.findAndUpdate(querier, listGE);    // make a list of ge to store more than one ge (GE1+GE2)???
//                    return;
//                }
//            }
//        } else {
//            // If querier not found or no matching GE, create a new one
//            GuardExp newGE = SieveGG(querier, newPolicies);
//            clockHashMap.put(querier, newGE);
//            return;
//        }
//    }
//    private int costMethod1(){
//        int a;
//    }
//
//    private int costMethod2(){
//        int b;
//    }
//
//    private GuardExp SieveGG (int querier, List<BEPolicy> allowPolicies){
//        if(allowPolicies == null){
//            allowPolicies = polper.retrievePolicies(String.valueOf(querier),
//                    PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
//        }
//
//        System.out.println("Querier #: " + querier + " with " + allowPolicies.size() + " allow policies");
//        BEExpression allowBeExpression = new BEExpression(allowPolicies);
//        Duration guardGen = Duration.ofMillis(0);
//
//        Instant fsStart = Instant.now();
//        SelectGuard gh = new SelectGuard(allowBeExpression, true);
//        Instant fsEnd = Instant.now();
//
//        System.out.println(gh.createGuardedQuery(true));
//        guardGen = guardGen.plus(Duration.between(fsStart, fsEnd));
//
//        System.out.println("Guard Generation time: " + guardGen + " Number of Guards: " + gh.numberOfGuards());
//
//        guardPersistor.insertGuard(gh.create(String.valueOf(querier), "user"));
//
//        return gh.create(String.valueOf(querier), "user");
//    }
//    public List<BEPolicy> fetchNewPolicies(Integer querier, Timestamp timestampGlobal) {
//        // Implementation for fetchNewPolicies function
//        List<BEPolicy> allowPolicies = polper.retrievePolicies(String.valueOf(querier),
//                PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
//        List<BEPolicy> newPolicies = null;
//        for (int i=0; i< allowPolicies.size(); i++) {
//            if (allowPolicies.get(i).getInserted_at().after(timestampGlobal)){
//                newPolicies.add(allowPolicies.get(i));
//            }
//        }
//        return newPolicies; // Placeholder, replace with actual implementation
//    }
//
//    public static void main(String[] args) {
//        // Example usage of CachingAlgorithm
//        long millis = Instant.now().toEpochMilli();
//        Timestamp timestampGlobal = new Timestamp(millis);
//        ClockHashMap<Integer, List<GuardExp>> clockHashMap = new ClockHashMap<>(3);
//        QueryPerformance e = new QueryPerformance();
//        List<QueryStatement> queries = e.getQueries(3, 9);
//        QueryStatement query = new QueryStatement();
//        CachingAlgorithm ca = new CachingAlgorithm();
//        ca.runAlgorithm(clockHashMap, query, timestampGlobal);
//    }
//}
