package org.mdb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.function.Consumer;

/* test assumes a local mongod is running */
public class QueryBuilderTest {
    private static Consumer<Document> printDocument = (d) -> System.out.println(d);
    private MongoClient client;
    private MongoDatabase db;
    private MongoCollection<Document> coll;
    private ClassLoader cl;
    private Document masterQuery;

    QueryBuilderTest(){
        client = MongoClients.create();
        db = client.getDatabase("demo");
        coll = db.getCollection("dummy");
        cl = getClass().getClassLoader();
        masterQuery = new Document().append("TOP_ELEM1", true).append("TOP_ELEM2", false);
    }

    void runTest(String file, Document masterQuery) throws Exception {
        Document rule = QueryBuilder.buildFromJson(cl.getResource(file).getFile(), masterQuery);
        System.out.println(rule.toJson());
        coll.aggregate(Arrays.asList(rule)).forEach(printDocument);
    }

    @Test
    @DisplayName("rule 1 no master query")
    void rule11() throws Exception {
        runTest("json/rule1.json", null);
    }

    @Test
    @DisplayName("rule 2 no master query")
    void rule21() throws Exception {
        runTest("json/rule2.json", null);
    }

    @Test
    @DisplayName("rule 3 no master query")
    void rule31() throws Exception {
        runTest("json/rule3.json", null);
    }

    @Test
    @DisplayName("rule 4 no master query")
    void rule41() throws Exception {
        runTest("json/rule4.json", null);
    }

    @Test
    @DisplayName("rule 5 no master query")
    void rule51() throws Exception {
        runTest("json/rule5.json", null);
    }

    @Test
    @DisplayName("rule 1 with master query")
    void rule12() throws Exception {
        runTest("json/rule1.json", masterQuery);
    }

    @Test
    @DisplayName("rule 2 with master query")
    void rule22() throws Exception {
        runTest("json/rule2.json", masterQuery);
    }

    @Test
    @DisplayName("rule 3 with master query")
    void rule32() throws Exception {
        runTest("json/rule3.json", masterQuery);
    }

    @Test
    @DisplayName("rule 4 with master query")
    void rule42() throws Exception {
        runTest("json/rule4.json", masterQuery);
    }

    @Test
    @DisplayName("rule 5 with master query")
    void rule52() throws Exception {
        runTest("json/rule5.json", masterQuery);
    }
}
