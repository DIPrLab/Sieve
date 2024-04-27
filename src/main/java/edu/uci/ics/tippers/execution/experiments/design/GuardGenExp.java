package edu.uci.ics.tippers.execution.experiments.design;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.mysql.MySQLConnectionManager;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.generation.policy.WiFiDataSet.PolicyUtil;
import edu.uci.ics.tippers.persistor.GuardPersistor;
import edu.uci.ics.tippers.persistor.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.SelectGuard;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import java.util.ArrayList;
import java.util.Collections;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;


/**
 * Experiment for measuring the time time taken for generating guards belonging to queriers
 * of different policy selectivities.
 * Experiment 1.1 in the paper
 */
public class GuardGenExp {

    PolicyPersistor polper;
    GuardPersistor guardPersistor;
    Connection connection;

    public GuardGenExp(){
        this.polper = PolicyPersistor.getInstance();
        this.guardPersistor = new GuardPersistor();
        this.connection = MySQLConnectionManager.getInstance().getConnection();
    }

//    private void writeExecTimes(int querier, int policyCount, int timeTaken){
//            String execTimesInsert = "INSERT INTO gg_results (querier, pCount, timeTaken) VALUES (?, ?, ?)";
//            try {
//                PreparedStatement eTStmt = connection.prepareStatement(execTimesInsert);
//                eTStmt.setInt(1, querier);
//                eTStmt.setInt(2, policyCount);
//                eTStmt.setInt(3, timeTaken);
//                eTStmt.execute();
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//    }

//    public void generateGuards(List<Integer> queriers){
//        Writer writer = new Writer();
//        StringBuilder result = new StringBuilder();
//        String fileName = "impexp.csv";
//        boolean first = true;
//        for(int querier: queriers) {
//            List<BEPolicy> allowPolicies = polper.retrievePolicies(String.valueOf(querier),
//                    PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
//            if(allowPolicies == null) continue;
//            System.out.println("Querier #: " + querier + " with " + allowPolicies.size() + " allow policies");
//            BEExpression allowBeExpression = new BEExpression(allowPolicies);
//            Duration guardGen = Duration.ofMillis(0);
//            Instant fsStart = Instant.now();
//            SelectGuard gh = new SelectGuard(allowBeExpression, true);
//            Instant fsEnd = Instant.now();
//            System.out.println(gh.createGuardedQuery(true));
//            guardGen = guardGen.plus(Duration.between(fsStart, fsEnd));
//            System.out.println("Guard Generation time: " + guardGen + " Number of Guards: " + gh.numberOfGuards());
//            guardPersistor.insertGuard(gh.create(String.valueOf(querier), "user"));
//            long noOfPredicates = gh.create().countNoOfPredicate();
//            result.append(querier).append(",")
//                    .append(allowPolicies.size()).append(",")
//                    .append(guardGen.toMillis()).append(",")
//                    .append(gh.numberOfGuards()).append(",")
//                    .append(noOfPredicates)//guard size
//                    .append("\n");
//            if(!first) writer.writeString(result.toString(), PolicyConstants.EXP_RESULTS_DIR, fileName);
//            else first = false;
//            result.setLength(0);
//        }
//    }

    public void generateGuards(List<Integer> queriers) {
        Writer writer = new Writer();
        StringBuilder result = new StringBuilder();
        String fileName = "attendance_generation1.csv";
        boolean first = true;

        for (int querier : queriers) {
            List<BEPolicy> allowPolicies = polper.retrievePolicies(String.valueOf(querier),
                    PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);

            if (allowPolicies == null) continue;

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

            // Recording execution time for each guard generation
            List<Long> gList = new ArrayList<>();
            gList.add(guardGen.toMillis());

            // Calculate average guard generation time, trimming outliers
            Duration gCost;
            if (gList.size() >= 3) {
                Collections.sort(gList);
                List<Long> clippedGList = gList.subList(1, gList.size() - 1);
                gCost = Duration.ofMillis(clippedGList.stream().mapToLong(i -> i).sum() / clippedGList.size());
            } else {
                gCost = Duration.ofMillis(gList.stream().mapToLong(i -> i).sum() / gList.size());
            }

            long noOfPredicates = gh.create().countNoOfPredicate();

            // Appending results to StringBuilder
            result.append(querier).append(",")
                    .append(allowPolicies.size()).append(",")
                    .append(gCost.toMillis()).append(",")
                    .append(gh.numberOfGuards()).append(",")
                    .append(noOfPredicates)// guard size
                    .append("\n");

            // Writing results to file
            if (!first) writer.writeString(result.toString(), PolicyConstants.EXP_RESULTS_DIR, fileName);
            else first = false;

            // Clearing StringBuilder for the next iteration
            result.setLength(0);
        }
    }


    public void runExperiment(){
        GuardGenExp ge = new GuardGenExp();
        PolicyUtil pg = new PolicyUtil();
        List<Integer> users = pg.getAllUsers(true);
        ge.generateGuards(users);
    }
}
