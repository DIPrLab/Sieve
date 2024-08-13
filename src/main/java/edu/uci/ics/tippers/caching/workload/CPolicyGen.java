package edu.uci.ics.tippers.caching.workload;

import edu.uci.ics.tippers.common.AttributeType;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.mysql.MySQLConnectionManager;
import edu.uci.ics.tippers.generation.policy.WiFiDataSet.PolicyGroupGen;
import edu.uci.ics.tippers.generation.policy.WiFiDataSet.PolicyUtil;
import edu.uci.ics.tippers.model.data.UserProfile;
import edu.uci.ics.tippers.model.policy.*;
import edu.uci.ics.tippers.persistor.PolicyPersistor;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.time.temporal.ChronoUnit;

public class CPolicyGen {

    private Connection connection;

    Random r;
    PolicyPersistor polper;
    PolicyUtil pg;

    private HashMap<Integer, List<String>> user_groups;
    private HashMap<Integer, String> user_profiles;
    private HashMap<String, List<Integer>> group_members;
    private HashMap<Integer, List<String>> location_clusters; //Forming random clusters of locations

    private TimeStampPredicate workingHours;
    private TimeStampPredicate nightDuskHours;
    private TimeStampPredicate nightDawnHours;

    private String START_WORKING_HOURS;
    private int DURATION_WORKING_HOURS;
    private String START_NIGHT_DUSK_HOURS;
    private String START_NIGHT_DAWN_HOURS;
    private int DURATION_NIGHT_DUSK_HOURS;
    private int DURATION_NIGHT_DAWN_HOURS;

    public CPolicyGen() {
        connection = MySQLConnectionManager.getInstance().getConnection();
        r = new Random();
        polper = PolicyPersistor.getInstance();
        pg = new PolicyUtil();

        location_clusters = new HashMap<>();
        List<String> all_locations = pg.getAllLocations();
        for (int i = 0; i < 10; i++) {
            List<String> locations = new ArrayList<>();
            location_clusters.put(i, locations);
        }
        for (String loc : all_locations) {
            int cluster = r.nextInt(10);
            location_clusters.get(cluster).add(loc);
        }


        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("config/execution/wifi_policy_gen.properties");
            Properties props = new Properties();
            if (inputStream != null) {
                props.load(inputStream);
                START_WORKING_HOURS = props.getProperty("w_start");
                DURATION_WORKING_HOURS = 120;
                workingHours = new TimeStampPredicate(pg.getDate("MIN"), pg.getDate("MAX"), START_WORKING_HOURS, DURATION_WORKING_HOURS);
                START_NIGHT_DUSK_HOURS = props.getProperty("n_dusk_start");
                DURATION_NIGHT_DUSK_HOURS = Integer.parseInt(props.getProperty("n_dusk_plus"));
                nightDuskHours = new TimeStampPredicate(pg.getDate("MIN"), pg.getDate("MAX"), START_NIGHT_DUSK_HOURS, DURATION_NIGHT_DUSK_HOURS);
                START_NIGHT_DAWN_HOURS = props.getProperty("n_dawn_start");
                DURATION_NIGHT_DAWN_HOURS = Integer.parseInt(props.getProperty("n_dawn_plus"));
                nightDawnHours = new TimeStampPredicate(pg.getDate("MIN"), pg.getDate("MAX"), START_NIGHT_DAWN_HOURS, DURATION_NIGHT_DAWN_HOURS);
            }
        } catch (IOException ie) {
            ie.printStackTrace();
        }
    }


    // Function select an element based on index and return an element
    private List<String> includeLocation(){
        if (Math.random() > (float) 1/location_clusters.size()){
            return location_clusters.get(r.nextInt(10));
        }
        else return null;
    }

    private LocalTime generateRandomStartTime(){
        final LocalTime START_RANGE = LocalTime.of(8, 0); // 8 AM
        final LocalTime END_RANGE = LocalTime.of(15, 0); // 3 PM
        final int INCREMENT_MINUTES = 30;
        Random random = new Random();
        int totalIncrements = (int) ChronoUnit.MINUTES.between(START_RANGE, END_RANGE) / INCREMENT_MINUTES;
        int randomIncrement = random.nextInt(totalIncrements + 1);
        return START_RANGE.plusMinutes(randomIncrement * INCREMENT_MINUTES);
    }

    public BEPolicy generateRandomPolicies(int querier, int owner_id, String owner_group, String owner_profile,
                                     TimeStampPredicate tsPred, String location, String action, int flag) {
        String policyID = UUID.randomUUID().toString();
        List<QuerierCondition> querierConditions = new ArrayList<>(Arrays.asList(
                new QuerierCondition(policyID, "policy_type", AttributeType.STRING, Operation.EQ, "user"),
                new QuerierCondition(policyID, "querier", AttributeType.STRING, Operation.EQ, String.valueOf(querier))));
        List<ObjectCondition> objectConditions = new ArrayList<>();
        if (owner_id != 0) {
            ObjectCondition owner = new ObjectCondition(policyID, PolicyConstants.USERID_ATTR, AttributeType.STRING,
                    String.valueOf(owner_id), Operation.EQ);
            objectConditions.add(owner);
        }
        if (owner_group != null) {
            ObjectCondition ownerGroup = new ObjectCondition(policyID, PolicyConstants.GROUP_ATTR, AttributeType.STRING,
                    owner_group, Operation.EQ);
            objectConditions.add(ownerGroup);
        }
        if (owner_profile != null) {
            ObjectCondition ownerProfile = new ObjectCondition(policyID, PolicyConstants.PROFILE_ATTR, AttributeType.STRING,
                    owner_profile, Operation.EQ);
            objectConditions.add(ownerProfile);
        }
        if (tsPred != null) {
            ObjectCondition datePred = new ObjectCondition(policyID, PolicyConstants.START_DATE, AttributeType.DATE,
                    tsPred.getStartDate().toString(), Operation.GTE, tsPred.getEndDate().toString(), Operation.LTE);
            objectConditions.add(datePred);
            ObjectCondition timePred = new ObjectCondition(policyID, PolicyConstants.START_TIME, AttributeType.TIME,
                    tsPred.parseStartTime(), Operation.GTE, tsPred.parseEndTime(), Operation.LTE);
            objectConditions.add(timePred);
        }
        if (location != null) {
            ObjectCondition locationPred = new ObjectCondition(policyID, PolicyConstants.LOCATIONID_ATTR, AttributeType.STRING,
                    location, Operation.EQ);
            objectConditions.add(locationPred);
        }
        if (objectConditions.isEmpty()) {
            System.out.println("Empty Object Conditions");
        }
        if(flag == 1){
            return new BEPolicy(policyID, objectConditions, querierConditions, "attendance-control",
                    action, new Timestamp(System.currentTimeMillis()));
        }else {
            return new BEPolicy(policyID, objectConditions, querierConditions, "space-usage",
                    action, new Timestamp(System.currentTimeMillis()));
        }

    }


    /*
    This function generates policies for all the users sampled by CUserGen class.
    Variable numPolicies indicates the # of policies defined by each user
    Calling the function generateRandomPolicies with flag == 1, means we are generating policies for AC scenario
    Policy Holders can only be students(undergrad, graduate)
    Querier is faculty
    Location can only be classrooms
     */
    public List<BEPolicy> generatePoliciesforAC(List<CUserGen.User> users){

        List<BEPolicy> policies = new ArrayList<>();
        for (CUserGen.User user: users){

            int numPolicies = 10;

            for (int i = 0; i < numPolicies; i++) {
                workingHours.setStartTime(generateRandomStartTime());
                workingHours.setEndTime(workingHours.getStartTime().plus(120, ChronoUnit.MINUTES));
                if (i<numPolicies){

                    if(user.getUserProfile().equals("graduate")){
                        List<Integer> possibleQueriers = new ArrayList<>();
                        for (CUserGen.User u : users) {
                            if (u.getUserProfile().equals("faculty") && user.getUserGroup().equals(u.getUserGroup())) {
                                possibleQueriers.add(u.getId());
                            }
                        }
                        Random r = new Random();
                        int index = r.nextInt(possibleQueriers.size());
                        BEPolicy policy = generateRandomPolicies(possibleQueriers.get(index),user.getId(),
                                user.getUserGroup(),user.getUserProfile(), workingHours, user.getUserGroup(),
                                PolicyConstants.ACTION_ALLOW, 1);
                        policies.add(policy);

                    }
                    if(user.getUserProfile().equals("undergrad")){
                        List<Integer> possibleQueriers = new ArrayList<>();
                        for (CUserGen.User u : users) {
                            if (u.getUserProfile().equals("faculty") && user.getUserGroup().equals(u.getUserGroup())) {
                                possibleQueriers.add(u.getId());
                            }
                        }
                        Random r = new Random();
                        int index = r.nextInt(possibleQueriers.size());
                        BEPolicy policy = generateRandomPolicies(possibleQueriers.get(index),user.getId(),
                                user.getUserGroup(),user.getUserProfile(), workingHours, user.getUserGroup(),
                                PolicyConstants.ACTION_ALLOW, 1);
                        policies.add(policy);

                    }
                }else {
                    List<Integer> possibleQueriers = new ArrayList<>();
                    for (CUserGen.User u : users) {
                        if (u != user) {
                            possibleQueriers.add(u.getId());
                        }
                    }
                    Random r = new Random();
                    int index = r.nextInt(possibleQueriers.size());
                    BEPolicy policy = generateRandomPolicies(possibleQueriers.get(index), user.getId(),
                            user.getUserGroup(), user.getUserProfile(), workingHours, null,
                            PolicyConstants.ACTION_ALLOW, 1);
                    policies.add(policy);
                }
            }
        }

        /*
        To print policies generated and store it in the DB
         */
//        for (BEPolicy policy : policies) {
//            System.out.println(policy.toString());
//        }
//        System.out.println();
//        polper.insertPolicy(policies);
        return policies;
    }

    /*
    This function generates policies for all the users sampled by CUserGen class.
    Variable numPolicies indicates the # of policies defined by each user
    Calling the function generateRandomPolicies with flag == 2, means we are generating policies for SU scenario
    Policy Holders can only be anyone
    Querier is faculty or staff
    Location can only be anything except for the forbidden locations
     */
    public List<BEPolicy> generatePoliciesforSU(List<CUserGen.User> users){

        List<BEPolicy> policies = new ArrayList<>();
        for (CUserGen.User user: users) {
            int numPolicies = 10;
            for (int i = 0; i < numPolicies; i++) {
                workingHours.setStartTime(LocalTime.of(0,0,0));
                workingHours.setEndTime(LocalTime.of(23,59,59));

                if (i<numPolicies){
                    if(user.getUserProfile().equals("graduate")){
                        List<Integer> possibleQueriers = new ArrayList<>();
                        for (CUserGen.User u : users) {
                            if (u.getUserProfile().equals("faculty") && user.getUserGroup().equals(u.getUserGroup())) {
                                possibleQueriers.add(u.getId());
                            }
                            if (u.getUserProfile().equals("staff") && user.getUserGroup().equals(u.getUserGroup())) {
                                possibleQueriers.add(u.getId());
                            }
                            Random r = new Random();
                            int index = r.nextInt(possibleQueriers.size());
                            BEPolicy policy = generateRandomPolicies(possibleQueriers.get(index),user.getId(),
                                    user.getUserGroup(),user.getUserProfile(), workingHours, user.getUserGroup(),
                                    PolicyConstants.ACTION_ALLOW, 2);
                            policies.add(policy);
                        }
                    }
                    if(user.getUserProfile().equals("undergraduate")){
                        List<Integer> possibleQueriers = new ArrayList<>();
                        for (CUserGen.User u : users) {
                            if (u.getUserProfile().equals("faculty") && user.getUserGroup().equals(u.getUserGroup())) {
                                possibleQueriers.add(u.getId());
                            }
                            if (u.getUserProfile().equals("staff") && user.getUserGroup().equals(u.getUserGroup())) {
                                possibleQueriers.add(u.getId());
                            }
                            Random r = new Random();
                            int index = r.nextInt(possibleQueriers.size());
                            BEPolicy policy = generateRandomPolicies(possibleQueriers.get(index),user.getId(),
                                    user.getUserGroup(),user.getUserProfile(), workingHours, user.getUserGroup(),
                                    PolicyConstants.ACTION_ALLOW, 2);
                            policies.add(policy);
                        }
                    }
                    if(user.getUserProfile().equals("staff")){
                        List<Integer> possibleQueriers = new ArrayList<>();
                        for (CUserGen.User u : users) {
                            if (u.getUserProfile().equals("faculty") && user.getUserGroup().equals(u.getUserGroup())) {
                                possibleQueriers.add(u.getId());
                            }
                            if (u.getUserProfile().equals("staff") && user.getUserGroup().equals(u.getUserGroup())) {
                                possibleQueriers.add(u.getId());
                            }
                            Random r = new Random();
                            int index = r.nextInt(possibleQueriers.size());
                            BEPolicy policy = generateRandomPolicies(possibleQueriers.get(index),user.getId(),
                                    user.getUserGroup(),user.getUserProfile(), workingHours, user.getUserGroup(),
                                    PolicyConstants.ACTION_ALLOW, 2);
                            policies.add(policy);
                        }
                    }
                    if(user.getUserProfile().equals("visitor")){
                        List<Integer> possibleQueriers = new ArrayList<>();
                        for (CUserGen.User u : users) {
                            if (u.getUserProfile().equals("faculty") && user.getUserGroup().equals(u.getUserGroup())) {
                                possibleQueriers.add(u.getId());
                            }
                            if (u.getUserProfile().equals("staff") && user.getUserGroup().equals(u.getUserGroup())) {
                                possibleQueriers.add(u.getId());
                            }
                            Random r = new Random();
                            int index = r.nextInt(possibleQueriers.size());
                            BEPolicy policy = generateRandomPolicies(possibleQueriers.get(index),user.getId(),
                                    user.getUserGroup(),user.getUserProfile(), workingHours, user.getUserGroup(),
                                    PolicyConstants.ACTION_ALLOW, 2);
                            policies.add(policy);
                        }
                    }
                    if(user.getUserProfile().equals("faculty")){
                        List<Integer> possibleQueriers = new ArrayList<>();
                        for (CUserGen.User u : users) {
                            if (u.getUserProfile().equals("faculty") && user.getUserGroup().equals(u.getUserGroup())) {
                                possibleQueriers.add(u.getId());
                            }
                            if (u.getUserProfile().equals("staff") && user.getUserGroup().equals(u.getUserGroup())) {
                                possibleQueriers.add(u.getId());
                            }
                            Random r = new Random();
                            int index = r.nextInt(possibleQueriers.size());
                            BEPolicy policy = generateRandomPolicies(possibleQueriers.get(index),user.getId(),
                                    user.getUserGroup(),user.getUserProfile(), workingHours, user.getUserGroup(),
                                    PolicyConstants.ACTION_ALLOW, 2);
                            policies.add(policy);
                        }
                    }
                    else {
                        List<Integer> possibleQueriers = new ArrayList<>();
                        for (CUserGen.User u : users) {
                            if (u != user) {
                                possibleQueriers.add(u.getId());
                            }
                        }
                        Random r = new Random();
                        int index = r.nextInt(possibleQueriers.size());
                        BEPolicy policy = generateRandomPolicies(possibleQueriers.get(index), user.getId(),
                                user.getUserGroup(), user.getUserProfile(), workingHours, null,
                                PolicyConstants.ACTION_ALLOW, 2);
                        policies.add(policy);
                    }
                }
            }
        }
        return policies;
    }



    public void runExpreriment () {
        CPolicyGen cpg = new CPolicyGen();
        CUserGen cUserGen = new CUserGen(1);
        List<CUserGen.User> users = cUserGen.retrieveUserDataForAC();
        List<BEPolicy> policies = cpg.generatePoliciesforAC(users);
        System.out.println("Total number of entries: " + users.size());
        System.out.println("Total number of entries: " + policies.size());
    }

}

