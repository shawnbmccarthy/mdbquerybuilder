package org.mdb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.function.Consumer;

/* test assumes a local mongod is running */
public class QueryBuilderTest {
    @Test
    void buildFromJson() throws Exception {

        Consumer<Document> printDocument = (d) -> System.out.println(d);

        ClassLoader cl = getClass().getClassLoader();
        MongoClient client = MongoClients.create();

        MongoDatabase db = client.getDatabase("demo");
        MongoCollection<Document> coll = db.getCollection("dummy");

        System.out.println("rule1");
        Document rule1 = QueryBuilder.buildFromJson(cl.getResource("json/rule1.json").getFile());
        System.out.println(rule1.toJson());
        coll.aggregate(Arrays.asList(rule1)).forEach(printDocument);
        System.out.println("rule2");
        Document rule2 = QueryBuilder.buildFromJson(cl.getResource("json/rule2.json").getFile());
        System.out.println(rule2.toJson());
        coll.aggregate(Arrays.asList(rule2)).forEach(printDocument);
        System.out.println("rule3");
        Document rule3 = QueryBuilder.buildFromJson(cl.getResource("json/rule3.json").getFile());
        System.out.println(rule3.toJson());
        coll.aggregate(Arrays.asList(rule3)).forEach(printDocument);
        System.out.println("rule4");
        Document rule4 = QueryBuilder.buildFromJson(cl.getResource("json/rule4.json").getFile());
        System.out.println(rule4.toJson());
        coll.aggregate(Arrays.asList(rule4)).forEach(printDocument);
        System.out.println("rule5");
        Document rule5 = QueryBuilder.buildFromJson(cl.getResource("json/rule5.json").getFile());
        System.out.println(rule5.toJson());
        coll.aggregate(Arrays.asList(rule5)).forEach(printDocument);
    }
}
