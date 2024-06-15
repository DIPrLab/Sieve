package edu.uci.ics.tippers.caching.costmodel;
import java.sql.Connection;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import edu.uci.ics.tippers.caching.CircularHashMap;
import edu.uci.ics.tippers.caching.ClockHashMap;
import edu.uci.ics.tippers.caching.workload.CPolicyGen;
import edu.uci.ics.tippers.caching.workload.CUserGen;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.mysql.MySQLConnectionManager;
import edu.uci.ics.tippers.execution.experiments.performance.QueryPerformance;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.generation.policy.WiFiDataSet.PolicyUtil;
import edu.uci.ics.tippers.model.guard.GenerateCandidate;
import edu.uci.ics.tippers.model.guard.GuardPart;
import edu.uci.ics.tippers.model.guard.SelectGuard;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.BooleanPredicate;
import edu.uci.ics.tippers.model.policy.ObjectCondition;
import edu.uci.ics.tippers.model.query.QueryStatement;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.persistor.GuardPersistor;
import edu.uci.ics.tippers.persistor.PolicyPersistor;

public class CostModelsExp <C,Q> {
    private Connection connection;
    Random r;
    PolicyPersistor polper;
    GuardPersistor guardPersistor;
    PolicyUtil pg;
    QueryPerformance e;
    Writer writer;
    StringBuilder result;
    String fileName;

    public CostModelsExp(){
        connection = MySQLConnectionManager.getInstance().getConnection();
        r = new Random();
        polper = PolicyPersistor.getInstance();
        pg = new PolicyUtil();
        this.guardPersistor = new GuardPersistor();
        e = new QueryPerformance();
        writer = new Writer();
        result = new StringBuilder();
        fileName = "redo_S5P1Q_M.csv";
        result.append("Querier"). append(",")
                .append("Cache log").append(",")
                .append("Time").append(",");
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
            if (lastestTimestamp.before(timestampGE)) {
                clockHashMap.update(querier);
                String answer = e.runGE(querier, query, guardExp);
                System.out.println(answer);
                result.append(querier).append(",")
                        .append("hit").append(",")
                        .append(1).append("\n");
                writer.writeString(result.toString(), PolicyConstants.EXP_RESULTS_DIR, fileName);
                newGE = guardExp;
            }else{
                List<BEPolicy> newPolicies = fetchNewPolicies(querier, timestampGE);
                int count = 0;
                for(BEPolicy policy: newPolicies){
                    List<ObjectCondition> policyConditions = policy.getObject_conditions();
                    outerloop:
                    for(ObjectCondition pcondition : policyConditions){
                        for(GuardPart guardPart : guardExp.getGuardParts()){
                            if(pcondition.getAttribute().equals("user_id") && guardPart.getGuard().getAttribute().equals("user_id")){
                                for(BooleanPredicate bp1 : pcondition.getBooleanPredicates()){
                                    for(BooleanPredicate bp2 : guardPart.getGuard().getBooleanPredicates()){
                                        if(bp1.getValue().equals(bp2.getValue())){
                                            count ++;
                                            break outerloop;
                                        }
                                    }
                                }
                            } else if (pcondition.getAttribute().equals("user_group") && guardPart.getGuard().getAttribute().equals("user_group")) {
                                for(BooleanPredicate bp1 : pcondition.getBooleanPredicates()){
                                    for(BooleanPredicate bp2 : guardPart.getGuard().getBooleanPredicates()){
                                        if(bp1.getValue().equals(bp2.getValue())){
                                            count ++;
                                            break outerloop;
                                        }
                                    }
                                }
                            }else if (pcondition.getAttribute().equals("user_profile") && guardPart.getGuard().getAttribute().equals("user_profile")){
                                for(BooleanPredicate bp1 : pcondition.getBooleanPredicates()){
                                    for(BooleanPredicate bp2 : guardPart.getGuard().getBooleanPredicates()){
                                        if(bp1.getValue().equals(bp2.getValue())){
                                            count ++;
                                            break outerloop;
                                        }
                                    }
                                }
                            } else if (pcondition.getAttribute().equals("location_id") && guardPart.getGuard().getAttribute().equals("location_id")) {
                                for(BooleanPredicate bp1 : pcondition.getBooleanPredicates()){
                                    for(BooleanPredicate bp2 : guardPart.getGuard().getBooleanPredicates()){
                                        if(bp1.getValue().equals(bp2.getValue())){
                                            count ++;
                                            break outerloop;
                                        }
                                    }
                                }
                            }else if (pcondition.getAttribute().equals("start_date") && guardPart.getGuard().getAttribute().equals("start_date")) {
                                GenerateCandidate gc = new GenerateCandidate();
                                Boolean flag = gc.mergeability(pcondition, guardPart.getGuard(), policy);
                                if(flag){
                                    count++;
                                    break outerloop;
                                }
                            }
                        }
                    }
                }
                if(count == newPolicies.size()){
                    Instant fsStart = Instant.now();
                    Duration totalTime = Duration.ofMillis(0);
                    newGE = SieveGG(querier, query);
                    clockHashMap.put(querier, newGE);
                    Instant fsEnd = Instant.now();
                    totalTime = totalTime.plus(Duration.between(fsStart, fsEnd));
                    result.append(querier).append(",")
                            .append("regenerate").append(",")
                            .append(totalTime).append("\n");
                    writer.writeString(result.toString(), PolicyConstants.EXP_RESULTS_DIR, fileName);
                }else{
                    Instant fsStart = Instant.now();
                    BEExpression allowBeExpression = new BEExpression(newPolicies);
                    Duration guardGen = Duration.ofMillis(0);
                    Duration totalTime = Duration.ofMillis(0);

                    SelectGuard gh = new SelectGuard(allowBeExpression, true);
                    Instant fsEnd = Instant.now();

                    System.out.println(gh.createGuardedQuery(true));
                    guardGen = guardGen.plus(Duration.between(fsStart, fsEnd));

                    System.out.println("Guard Generation time: " + guardGen + " Number of Guards: " + gh.numberOfGuards());

                    guardPersistor.insertGuard(gh.create(String.valueOf(querier), "user"));

                    newGE = gh.create(String.valueOf(querier), "user");
                    for (GuardPart gp: newGE.getGuardParts()){
                        guardExp.getGuardParts().add(gp);
                    }

                    String answer = e.runGE(querier, query, guardExp);
                    System.out.println(answer);
                    clockHashMap.put(querier, guardExp);
                    Instant totalEnd = Instant.now();
                    totalTime = totalTime.plus(Duration.between(fsStart, totalEnd));
                    result.append(querier).append(",")
                            .append("updation").append(",")
                            .append(totalTime).append("\n");
                    writer.writeString(result.toString(), PolicyConstants.EXP_RESULTS_DIR, fileName);
                }
            }
        }else{
            // If querier not found or no matching GE, create a new one
            newGE = SieveGG(querier, query);
            if (newGE == null){
                return;
            }else {
                clockHashMap.put(querier, newGE);
                result.append(querier).append(",")
                        .append("miss").append(",")
                        .append(0).append("\n");
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
//    private int costMethod1(){
//        int a;
//    }
//
//    private int costMethod2(){
//        int b;
//    }

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

        guardPersistor.insertGuard(gh.create(String.valueOf(querier), "user"));

        System.out.println(e.runBEPolicies(querier,query,allowPolicies));

        return gh.create(String.valueOf(querier), "user");
    }
    public List<BEPolicy> fetchNewPolicies(String querier, Timestamp timestampGlobal) {
        // Implementation for fetchNewPolicies function
        List<BEPolicy> allowPolicies = polper.retrievePolicies(querier,
                PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
        List<BEPolicy> newPolicies = new ArrayList<>();
        for (BEPolicy policy : allowPolicies) {
            if (policy.getInserted_at().after(timestampGlobal)){
                newPolicies.add(policy);
            }
        }
        return newPolicies; // Placeholder, replace with actual implementation
    }

    public static void main(String[] args) {

        ClockHashMap<String, GuardExp> clockHashMap = new ClockHashMap<>(3);
        QueryPerformance e = new QueryPerformance();

        CUserGen cUserGen = new CUserGen();
        List<CUserGen.User> users = cUserGen.retrieveUserData();
        Iterator<CUserGen.User> iterator = users.iterator();
        while (iterator.hasNext()) {
            CUserGen.User user = iterator.next();
            if (!user.getUserId().equals("958") ) {
                iterator.remove();
            }
        }

        CPolicyGen cpg = new CPolicyGen();
        List<BEPolicy> policies = cpg.generatePolicies(users);

        System.out.println("Total number of entries: " + users.size());
        System.out.println("Total number of entries: " + policies.size());

        List<QueryStatement> query = new ArrayList<>();
        for (QueryStatement q : query){
            q.setQuery("start_date >= \"2018-02-01\" AND start_date <= \"2018-04-02\" and start_time >= \"00:00\" " +
                    "AND start_time <= \"20:00\" AND location_id IN (\"3142-clwa-2019\")");
            q.setId(1);
            q.setSelectivity(0);
            q.setTemplate(1);
        }

        System.out.println("Total number of entries: " + users.size());
        System.out.println("Total number of policies: " + policies.size());
        System.out.println("Total number of queries: " + query.size());


    }
}
