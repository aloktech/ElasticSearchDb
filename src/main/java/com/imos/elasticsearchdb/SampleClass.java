/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.imos.elasticsearchdb;

import static com.imos.elasticsearchdb.utils.ElasticSearchDBConstants.*;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author Alok
 */
public class SampleClass {

    SampleClass() {
        String query = null;
        //BufferedReader reader = new BufferedReader(new InputStreamReader(ClassLoader.getSystemResourceAsStream("com/imos/sample/query.json")));
        BufferedReader reader;
        StringBuilder builder = new StringBuilder();
        try {
            reader = new BufferedReader(new FileReader("src/main/resources/com/imos/sample/query.json"));
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
                builder.append("\n");
            }
            query = builder.toString();
            System.out.println(query);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(SampleClass.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(SampleClass.class.getName()).log(Level.SEVERE, null, ex);
        }

        // Construct a new Jest client according to configuration via factory
        String connectionUrl = "http://localhost:9200";
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder(connectionUrl)
                .multiThreaded(true)
                .build());
        JestClient client = factory.getObject();

//        new CreateIndex.Builder("articles").build();
//        try {
//            client.execute(new CreateIndex.Builder("articles").build());
//        } catch (Exception ex) {
//            Logger.getLogger(SampleClass.class.getName()).log(Level.SEVERE, null, ex);
//        }
        Article source = new Article();
        source.setAuthor("John Ronald Reuel Tolkien");
        source.setContent("The Lord of the Rings is an epic high fantasy novel");

        Index index = new Index.Builder(source).index("articles").type("article").build();
//        try {
//            client.execute(index);
//        } catch (Exception ex) {
//            Logger.getLogger(SampleClass.class.getName()).log(Level.SEVERE, null, ex);
//        }

        Search search = (Search) new Search.Builder(query)
                // multiple index or types can be added.
                .addIndex("articles")
                .addType("article")
                .build();

        JestResult result = null;
        try {
            result = client.execute(search);
            System.out.println(result.getJsonString());
        } catch (Exception ex) {
            Logger.getLogger(SampleClass.class.getName()).log(Level.SEVERE, null, ex);
        }

        JSONObject hits = getResults(result.getJsonString());
        if (hits.has(HITS)) {
            JSONArray array = hits.getJSONArray(HITS);

        }
        
    }

    public static void main(String[] args) {

        new SampleClass();
    }

    public final JSONObject getResults(String result) {
        JSONObject root = new JSONObject(result);
        if (root.has(HITS)) {
            return root.getJSONObject(HITS);
        }

        return root;
    }

}
