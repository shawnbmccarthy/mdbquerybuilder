package org.mdb;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import org.bson.Document;
import org.bson.types.Decimal128;

public class QueryBuilder {
    public static final String CONDITION_FIELD = "condition";
    public static final String OPERATOR_FIELD = "operator";

    public static Document buildFromJson(String jsonFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(new File(jsonFile));
        return new Document("$match", processRule(root));
    }

    private static final Document processRule(JsonNode node){
        // this is the entire rule file, essentially
        Document rule = new Document();
        if(node.has(CONDITION_FIELD)){
            if(!node.get("isDisabled").asBoolean()){
                String key = "$" + node.get("condition").asText();
                List<Document> a = new ArrayList<Document>();
                JsonNode rules = node.get("rules");
                for(final JsonNode r : rules) {
                    Document item = processRule(r);
                    if(item != null) {
                        a.add(item);
                    }
                }
                if(a.size() > 0) {
                    rule.append(key, a);
                }
            } else {
                return null;
            }
        } else if(node.has(OPERATOR_FIELD)) {
            String operator = node.get("operator").asText();
            String datatype = node.get("datatype").asText();
            String value = node.get("value").asText();
            String label = node.get("label").asText();

            if (operator.equals("=")) {
                rule.append(label, getTypedValue(value, datatype));
            } else if (operator.equals(">=")) {
                rule.append("$gte", new Document(label, getTypedValue(value, datatype)));
            } else if(operator.equals("<=")) {
                rule.append("$lte", new Document(label, getTypedValue(value, datatype)));
            } else if(operator.equals("in")) {
                rule.append("$in", new Document(label, value));
            } else if(operator.equals("not_in")){
                rule.append("$nin", new Document(label, value));
            } else if(operator.equals("is_not_null")){
                rule.append(label, new Document("$exists", true));
            } else if(operator.equals("is_null")){
                rule.append(label, new Document("$exists", false));
            } else if(operator.equals("!=_with_null")){
                //TODO: fix (not what this means)
                System.err.println("found !=_with_null, this will not be correctly matched");
                rule.append(label, getTypedValue(value, datatype));
            } else {
                System.err.println("WARNING: operator not supported: " + operator);
                rule.append(label, getTypedValue(value, datatype));
            }
        }
        return rule;
    }

    private static final Object getTypedValue(String value, String type){
        //short circuit
        if(value.equals("null") || value == null) {
            return null;
        }
        if(type.equals("Decimal128")){
            return new Decimal128(new BigDecimal(value));
        } else if(type.equals("String")){
            return value;
        } else {
            System.err.println("WARNING: type not supported: " + type);
            return value;
        }
    }
}