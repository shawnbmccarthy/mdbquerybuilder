package org.mdb;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import org.bson.Document;
import org.bson.types.Decimal128;

public class QueryBuilder {
    private static final String CONDITION_FIELD = "condition";
    private static final String OPERATOR_FIELD = "operator";

    public static Document buildFromJson(String jsonFile, Document masterQuery) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(new File(jsonFile));
        Document filters = processRule(root);

        if(masterQuery == null) {
            return new Document("$match", filters);
        } else {
            Document query = new Document();
            masterQuery.putAll(filters);
            query.put("$match", masterQuery);
            return query;
        }
    }

    /*
     * recursively process conditions, with the base case of an operator which adds a single key/value map to
     * the current stacks document and return it
     */
    private static Document processRule(JsonNode node){
        Document rule = new Document();
        if(node.has(CONDITION_FIELD)){
            if(!node.get("isDisabled").asBoolean()){
                String key = "$" + node.get("condition").asText();
                List<Document> a = new ArrayList<>();
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

            switch (operator){
                case "=":
                    rule.append(label, getTypedValue(value, datatype));
                    break;
                case ">=":
                    rule.append(label, new Document("$gte", getTypedValue(value, datatype)));
                    break;
                case "<=":
                    rule.append(label, new Document("$lte", getTypedValue(value, datatype)));
                    break;
                case "in":
                    rule.append(label, new Document("$in", Arrays.asList(value.split(","))));
                    break;
                case "not_in":
                    rule.append(label, new Document("$nin", Arrays.asList(value.split(","))));
                    break;
                case "is_not_null":
                    rule.append(label, new Document("$exists", true));
                    break;
                case "is_null":
                    rule.append(label, new Document("$exists", false));
                    break;
                case "!=_with_null":
                    //TODO: fix (not what this means)
                    System.err.println("WARNING: found !=_with_null, will not process");
                    //rule.append(label, getTypedValue(value, datatype));
                    break;
                default:
                    System.err.println("WARNING: operator not supported, using equal(=): " + operator);
                    //rule.append(label, getTypedValue(value, datatype));
            }
        }
        return rule;
    }

    private static Object getTypedValue(String value, String type){
        //short circuit
        if(value.equals("null") || value == null) {
            return null;
        }

        switch (type){
            case "Decimal128":
                return new Decimal128(new BigDecimal(value));
            case "String":
                return value;
            default:
                System.err.println("WARNING: type not supported: " + type);
                return value;
        }
    }
}