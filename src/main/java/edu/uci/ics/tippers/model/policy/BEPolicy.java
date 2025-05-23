package edu.uci.ics.tippers.model.policy;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import edu.uci.ics.tippers.common.PolicyConstants;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by cygnus on 9/25/17.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BEPolicy{

    @JsonProperty("id")
    private String id;

    @JsonProperty("description")
    private String description;

    @JsonProperty("metadata")
    private String metadata;

    /**
     * 1) user = Alice ^ loc = 2065
     * 2) user = Bob ^ time > 5pm
     * 3) user = Alice ^ 5 pm < time < 7 pm
     */
    @JsonProperty("object_conditions")
    private List<ObjectCondition> object_conditions;

    /**
     * 1) querier = John
     * 2) querier = John ^ time = 6 pm
     * 3) querier = John ^ location = 2065
     */
    @JsonProperty("querier_conditions")
    private List<QuerierCondition> querier_conditions;

    /**
     * 1) Analysis
     * 2) Concierge
     * 3) Noodle
     */
    @JsonProperty("purpose")
    private String purpose;

    /**
     * 1) Allow
     * 2) Deny
     */
    @JsonProperty("action")
    private String action;

    @JsonIgnore
    private double estCost;

    @JsonProperty("inserted_at")
    @JsonFormat(shape= JsonFormat.Shape.STRING, pattern="yyyy-MM-dd HH:mm:ss", locale = "America/Phoenix")
    private Timestamp inserted_at;

    public BEPolicy(){
        this.object_conditions = new ArrayList<ObjectCondition>();
        this.querier_conditions = new ArrayList<QuerierCondition>();
    }

    public BEPolicy(BEPolicy bePolicy){
        this.id = bePolicy.getId();
        this.description = bePolicy.getDescription();
        this.metadata = bePolicy.getMetadata();
        this.object_conditions = new ArrayList<ObjectCondition>(bePolicy.getObject_conditions().size());
        for(ObjectCondition oc: bePolicy.getObject_conditions()){
            this.object_conditions.add(new ObjectCondition(oc));
        }
        this.action = bePolicy.getAction();
        this.purpose = bePolicy.getPurpose();

    }

    public BEPolicy(String id, String description, List<ObjectCondition> object_conditions, List<QuerierCondition> querier_conditions, String purpose, String action) {
        this.id = id;
        this.description = description;
        this.object_conditions = object_conditions;
        this.querier_conditions = querier_conditions;
        this.purpose = purpose;
        this.action = action;
    }
    public BEPolicy(String id, List<ObjectCondition> object_conditions, List<QuerierCondition> querier_conditions, String purpose, String action, Timestamp inserted_at) {
        this.id = id;
        this.object_conditions = object_conditions;
        this.querier_conditions = querier_conditions;
        this.purpose = purpose;
        this.action = action;
        this.inserted_at = inserted_at;
    }


    public BEPolicy(List<ObjectCondition> objectConditions){

        this.object_conditions = objectConditions;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getMetadata() {
        return metadata;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }

    public List<ObjectCondition> getObject_conditions() {
        return object_conditions;
    }

    public void setObject_conditions(List<ObjectCondition> object_conditions) {
        this.object_conditions = object_conditions;
    }

    public List<QuerierCondition> getQuerier_conditions() {
        return querier_conditions;
    }

    public void setQuerier_conditions(List<QuerierCondition> querier_conditions) {
        this.querier_conditions = querier_conditions;
    }

    public String getPurpose() {
        return purpose;
    }

    public void setPurpose(String purpose) {
        this.purpose = purpose;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Timestamp getInserted_at() {
        return inserted_at;
    }

    public void setInserted_at(Timestamp inserted_at) {
        this.inserted_at = inserted_at;
    }

    public double getEstCost() {
        return estCost;
    }

    public void setEstCost(double estCost) {
        this.estCost = estCost;
    }

    public List<String> retrieveObjCondAttributes(){
        Set<String> attrs = new HashSet<>();
        for(ObjectCondition oc: object_conditions) {
            attrs.add(oc.getAttribute());
        }
        return new ArrayList<>(attrs);
    }


    public static BEPolicy parseJSONObject(String jsonData){
        ObjectMapper objectMapper = new ObjectMapper();
        BEPolicy bePolicy = null;
        try {
            bePolicy = objectMapper.readValue(jsonData, BEPolicy.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bePolicy;
    }

    //TODO: Make it a single method for both subclasses taking parent class as parameter
    public String serializeObjectConditions(List<ObjectCondition> bcs){
        StringBuilder result = new StringBuilder();
        result.append("[ ");
        for(int i = 0; i < bcs.size(); i++){
            result.append(bcs.get(i).print());
        }
        result.append(" ],");
        return result.toString();
    }

    public String serializeQuerierConditions(List<QuerierCondition> bcs){
        StringBuilder result = new StringBuilder();
        result.append("[ ");
        for(int i = 0; i < bcs.size(); i++){
            result.append(bcs.get(i).print());
        }
        result.append(" ],");
        return result.toString();
    }

    /**
     * Check if a set of object conditions is contained in a policy
     * @param objectConditionSet
     * @return true if all object conditions are contained in the policy, false otherwise
     */
    public boolean containsObjCond(Set<ObjectCondition> objectConditionSet){
        return Sets.newHashSet(this.object_conditions).containsAll(objectConditionSet);
    }

    public boolean containsObjCond(ObjectCondition oc){
        List<ObjectCondition> contained = this.object_conditions.stream()
                .filter(objCond -> objCond.equalsWithoutId(oc))
                .collect(Collectors.toList());
        return contained.size() != 0;
    }

    /**
     * Checks for matching object conditions in the policy based on attribute and value (not id)
     * @param oc
     */
    public void deleteObjCond(ObjectCondition oc){
        List<ObjectCondition> toRemove = this.object_conditions.stream()
                .filter(objCond -> objCond.getAttribute().equalsIgnoreCase(oc.getAttribute()))
                .filter(objCond -> objCond.compareTo(oc) == 0)
                .collect(Collectors.toList());
        if(this.object_conditions.size() == toRemove.size()) return;
        this.object_conditions.removeAll(toRemove);
    }

    /**
     * @return String of the query constructed based on the policy in CNF
     */
    public String createQueryFromObjectConditions(){
        StringBuilder query = new StringBuilder();
        String delim = "";
        for (ObjectCondition oc: this.getObject_conditions()) {
            query.append(delim);
            query.append(oc.print());
            delim = PolicyConstants.CONJUNCTION;
        }
        return query.toString();

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BEPolicy bePolicy = (BEPolicy) o;
        return Objects.equals(id, bePolicy.id) &&
                Objects.equals(new HashSet<>(object_conditions), new HashSet<>(bePolicy.object_conditions));
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, object_conditions);
    }

    public boolean equalsWithoutId(BEPolicy bePolicy){
        int oc_size = this.getObject_conditions().size();
        for (ObjectCondition oc: this.getObject_conditions()) {
            for (ObjectCondition bec: bePolicy.getObject_conditions()) {
                if(oc.equalsWithoutId(bec)) {
                    oc_size -= 1;
                    break;
                }
            }
        }
        return oc_size == 0;
    }

    /**
     * For each attribute,
     * if the boolean predicates are range predicates, it selects the maximum for the >= predicate and minimum
     * for the <= predicate (tightens bounds)
     * @return
     */
    public void cleanDuplicates() {
        StringBuilder query = new StringBuilder();
        String delim = "";
        Map<String, ObjectCondition> dupRemoval = new HashMap<>();
        for (ObjectCondition oc : this.getObject_conditions()) {
            if (dupRemoval.containsKey(oc.getAttribute())) {
                if (oc.getBooleanPredicates().get(0).compareOnType
                        (dupRemoval.get(oc.getAttribute()).getBooleanPredicates().get(0), oc.getAttribute()) > 0) {
                    dupRemoval.get(oc.getAttribute()).getBooleanPredicates().get(0).setValue
                            (oc.getBooleanPredicates().get(0).getValue());
                }
                if (oc.getBooleanPredicates().get(1).compareOnType
                        (dupRemoval.get(oc.getAttribute()).getBooleanPredicates().get(1), oc.getAttribute()) < 0) {
                    dupRemoval.get(oc.getAttribute()).getBooleanPredicates().get(1).setValue
                            (oc.getBooleanPredicates().get(1).getValue());
                }
            } else {
                dupRemoval.put(oc.getAttribute(), oc);
            }
        }
        this.getObject_conditions().clear();
        this.getObject_conditions().addAll(dupRemoval.values());
    }


    /**
     * Calculates the powerset of the object conditions of the policy which includes empty set and complete set
     * @return Set of object condition sets
     */
    public Set<Set<ObjectCondition>> calculatePowerSet() {
        Set<ObjectCondition> objectConditionSet = new HashSet<>(this.getObject_conditions());
        Set<Set<ObjectCondition>> result =  Sets.powerSet(objectConditionSet);
        return result;
    }

    public double estimateTableScanCost() {
        return PolicyConstants.getNumberOfTuples() * (
                    PolicyConstants.ROW_EVALUATE_COST * PolicyConstants.NUMBER_OF_PREDICATES_EVALUATED *
                            countNumberOfPredicates());
    }


    public ObjectCondition getIndexScanPredicate(){
        ObjectCondition selected = this.getObject_conditions().get(0);
        for (ObjectCondition oc : this.getObject_conditions()) {
            if (oc.computeL() < selected.computeL())
                selected = oc;
        }
        return selected;
    }

    /**
     * If eval
     * Estimates the upper bound of the cost of evaluating an individual policy by adding up
     * io block read cost * selectivity of lowest selectivity predicate * D +
     * (D * selectivity of lowest selectivity predicate *  row evaluate cost * 2  * alpha * number of predicates)
     * alpha is a parameter which determines the number of predicates that are evaluated in the policy (e.g., 2/3)
     * @return
     */
    public double estimateCost(Boolean evalOnly) {
        ObjectCondition selected = this.getObject_conditions().get(0);
        for (ObjectCondition oc : this.getObject_conditions()) {
            if (oc.computeL() < selected.computeL())
                selected = oc;
        }
        double cost;
        if(!evalOnly){
            cost = PolicyConstants.getNumberOfTuples() * selected.computeL() *(PolicyConstants.IO_BLOCK_READ_COST  +
                    PolicyConstants.ROW_EVALUATE_COST * PolicyConstants.NUMBER_OF_PREDICATES_EVALUATED *
                            countNumberOfPredicates());
        }
        else{
            cost = PolicyConstants.getNumberOfTuples() * selected.computeL() *
                    PolicyConstants.ROW_EVALUATE_COST * PolicyConstants.NUMBER_OF_PREDICATES_EVALUATED *
                            countNumberOfPredicates();
        }
        return cost;
    }



    /**
     * Selectivity of a conjunctive expression
     * e.g., A = u and B = v
     * sel = set (A) * sel (B)
     * with independence assumption
     * @return
     */
    public float computeL(){
        float selectivity = 1;
        for (ObjectCondition obj: this.getObject_conditions()) {
            selectivity *= obj.computeL();
        }
        return selectivity;
    }

    /**
     * Estimates the cost of evaluating an individual policy by index scan on the given predicate
     * (for the purpose of extension) by adding up
     * io block read cost * selectivity of predicate on a given attribute * D +
     * (D * selectivity of predicate on a given attribute *  row evaluate cost * 2  * alpha * number of predicates)
     * alpha is a parameter which determines the number of predicates that are evaluated in the policy (e.g., 2/3)
     */
    public double estimateCostForExtension(String attribute) {
        ObjectCondition selected = this.getObject_conditions().stream().filter(o -> o.getAttribute().equals(attribute)).findFirst().get();
        double cost = PolicyConstants.getNumberOfTuples() * selected.computeL() *(PolicyConstants.IO_BLOCK_READ_COST  +
                PolicyConstants.ROW_EVALUATE_COST * PolicyConstants.NUMBER_OF_PREDICATES_EVALUATED *
                        countNumberOfPredicates());
        return cost;
    }

    /**
     * Equality predicates are counted as 1
     * Range predicates are counted as 2
     * @return
     */
    public int countNumberOfPredicates() {
        int count = 0;
        for (int j = 0; j < this.getObject_conditions().size(); j++) {
            ObjectCondition oc = this.getObject_conditions().get(j);
            if (oc.getAttribute().equalsIgnoreCase(PolicyConstants.USERID_ATTR) ||
                    oc.getAttribute().equalsIgnoreCase(PolicyConstants.LOCATIONID_ATTR) ||
                    oc.getAttribute().equalsIgnoreCase(PolicyConstants.ACTIVITY_ATTR))
                count += 1;
            else count += 2;
        }
        return count;
    }


    public boolean typeOfPolicy(){
        for (QuerierCondition querier_condition : this.querier_conditions)
            if (querier_condition.getAttribute().equalsIgnoreCase("policy_type"))
                return querier_condition.checkTypeOfPolicy();
        return false;
    }

    public String fetchQuerier(){
        for (QuerierCondition querier_condition : this.querier_conditions)
            if (querier_condition.getAttribute().equalsIgnoreCase("querier"))
                return querier_condition.getBooleanPredicates().get(0).getValue();
            return null;
    }

    /**
     * Returns the owner of the tuple for which policy is specified based on the object condition
     * declared on user_id attribute
     * @return
     */
    public int fetchOwner(){
        for (ObjectCondition objectCondition : this.object_conditions)
            if (objectCondition.getAttribute().equalsIgnoreCase(PolicyConstants.USERID_ATTR))
                return Integer.parseInt(objectCondition.getBooleanPredicates().get(0).getValue());
        return 0;
    }

    public String fetchLocation() {
        for (ObjectCondition objectCondition : this.object_conditions)
            if (objectCondition.getAttribute().equalsIgnoreCase(PolicyConstants.LOCATIONID_ATTR))
                return objectCondition.getBooleanPredicates().get(0).getValue();
        return null;

    }

    public String fetchGroup() {
        for (ObjectCondition objectCondition : this.object_conditions)
            if (objectCondition.getAttribute().equalsIgnoreCase(PolicyConstants.GROUP_ATTR))
                return objectCondition.getBooleanPredicates().get(0).getValue();
        return null;

    }

    public String fetchProfile() {
        for (ObjectCondition objectCondition : this.object_conditions)
            if (objectCondition.getAttribute().equalsIgnoreCase(PolicyConstants.PROFILE_ATTR))
                return objectCondition.getBooleanPredicates().get(0).getValue();
        return null;

    }

    public String fetchTemperatureLowValue() {
        for (ObjectCondition objectCondition : this.object_conditions)
            if (objectCondition.getAttribute().equalsIgnoreCase(PolicyConstants.TEMPERATURE_ATTR))
                return objectCondition.getBooleanPredicates().get(0).getValue();
        return null;
    }

    public String fetchTemperatureHighValue() {
        for (ObjectCondition objectCondition : this.object_conditions)
            if (objectCondition.getAttribute().equalsIgnoreCase(PolicyConstants.TEMPERATURE_ATTR))
                return objectCondition.getBooleanPredicates().get(1).getValue();
        return null;
    }

    public String fetchEnergyLowValue() {
        for (ObjectCondition objectCondition : this.object_conditions)
            if (objectCondition.getAttribute().equalsIgnoreCase(PolicyConstants.ENERGY_ATTR))
                return objectCondition.getBooleanPredicates().get(0).getValue();
        return null;
    }

    public String fetchEnergyHighValue() {
        for (ObjectCondition objectCondition : this.object_conditions)
            if (objectCondition.getAttribute().equalsIgnoreCase(PolicyConstants.ENERGY_ATTR))
                return objectCondition.getBooleanPredicates().get(1).getValue();
        return null;
    }

    public String fetchActivity() {
        for (ObjectCondition objectCondition : this.object_conditions)
            if (objectCondition.getAttribute().equalsIgnoreCase(PolicyConstants.ACTIVITY_ATTR))
                return objectCondition.getBooleanPredicates().get(0).getValue();
        return null;
    }

    /**
     * Collects the beginning and end values of 'start' date object condition
     * @return
     */
    public List<java.sql.Date> fetchDate() {
        SimpleDateFormat sdf = new SimpleDateFormat(PolicyConstants.DATE_FORMAT);
        List<java.sql.Date> start = new ArrayList<>();
        for (ObjectCondition objectCondition : this.object_conditions)
            if (objectCondition.getAttribute().equalsIgnoreCase(PolicyConstants.START_DATE)) {
                try {
                    Date sb = sdf.parse(objectCondition.getBooleanPredicates().get(0).getValue());
                    start.add(new java.sql.Date(sb.getTime()));
                    Date se = sdf.parse(objectCondition.getBooleanPredicates().get(1).getValue());
                    start.add(new java.sql.Date(se.getTime()));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        return start;
    }


    /**
     * Collects the beginning and end values of 'start' time object condition
     * @return
     */
    public List<java.sql.Time> fetchTime() {
        SimpleDateFormat sdf = new SimpleDateFormat(PolicyConstants.TIME_FORMAT);
        List<java.sql.Time> start = new ArrayList<>();
        for (ObjectCondition objectCondition : this.object_conditions)
            if (objectCondition.getAttribute().equalsIgnoreCase(PolicyConstants.START_TIME)) {
                try {
                    Date sb = sdf.parse(objectCondition.getBooleanPredicates().get(0).getValue());
                    start.add(new java.sql.Time(sb.getTime()));
                    Date se = sdf.parse(objectCondition.getBooleanPredicates().get(1).getValue());
                    start.add(new java.sql.Time(se.getTime()));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        return start;
    }


    @Override
    public String toString() {
        return "BEPolicy{" +
                "id='" + id + '\'' +
                ", description='" + description + '\'' +
                ", metadata='" + metadata + '\'' +
                ", object_conditions=" + object_conditions +
                ", querier_conditions=" + querier_conditions +
                ", purpose='" + purpose + '\'' +
                ", action='" + action + '\'' +
                ", inserted_at=" + inserted_at +
                '}';
    }
}