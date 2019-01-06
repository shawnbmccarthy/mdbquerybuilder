package org.mdb;

import org.junit.jupiter.api.Test;

public class QueryBuilderTest {
    @Test
    void buildFromJson() throws Exception {
        ClassLoader cl = getClass().getClassLoader();
        System.out.println("rule1");
        System.out.println(QueryBuilder.buildFromJson(cl.getResource("json/rule1.json").getFile()));
        System.out.println("rule2");
        System.out.println(QueryBuilder.buildFromJson(cl.getResource("json/rule2.json").getFile()));
        System.out.println("rule3");
        System.out.println(QueryBuilder.buildFromJson(cl.getResource("json/rule3.json").getFile()));
        System.out.println("rule4");
        System.out.println(QueryBuilder.buildFromJson(cl.getResource("json/rule4.json").getFile()));
        System.out.println("rule5");
        System.out.println(QueryBuilder.buildFromJson(cl.getResource("json/rule5.json").getFile()));
    }
}
