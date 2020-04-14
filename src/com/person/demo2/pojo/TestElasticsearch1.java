package com.person.demo2.pojo;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

//使用客户端操作elasticsearch
// 判断/创建/删除索引
public class TestElasticsearch1 {

    private static RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost("localhost", 9200, "http")
            ));
    private static String indexName = "hello";



    public static void main(String[] args) throws IOException {
        //确保索引存在
        if(!checkExistIndex(indexName)){
            createIndex(indexName);
        }
        //准备数据
        Product product = new Product();
        product.setId(1);
        product.setName("product 1");

        //增加文档
        addDocument(product);

        //获取文档
        getDocument(1);


        client.close();

    }


    private static void getDocument(int id) throws IOException {
        // TODO Auto-generated method stub
        GetRequest request = new GetRequest(
                indexName,
                "product",
                String.valueOf(id));

        GetResponse response = client.get(request);

        if(!response.isExists()){
            System.out.println("检查到服务器上 "+"id="+id+ "的文档不存在");
        }
        else{
            String source = response.getSourceAsString();
            System.out.print("获取到服务器上 "+"id="+id+ "的文档内容是：");

            System.out.println(source);

        }

    }

    private static void addDocument(Product product) throws IOException {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", product.getName());
        IndexRequest indexRequest = new IndexRequest(indexName, "product", String.valueOf(product.getId()))
                .source(jsonMap);
        client.index(indexRequest);
        System.out.println("已经向ElasticSearch服务器增加产品："+product);
    }

    private static boolean checkExistIndex(String indexName) throws IOException {
        boolean result =true;
        try {

            OpenIndexRequest openIndexRequest = new OpenIndexRequest(indexName);
            client.indices().open(openIndexRequest).isAcknowledged();

        } catch (ElasticsearchStatusException ex) {
            String m = "Elasticsearch exception [type=index_not_found_exception, reason=no such index]";
            if (m.equals(ex.getMessage())) {
                result = false;
            }
        }
        if(result)
            System.out.println("索引:" +indexName + " 是存在的");
        else
            System.out.println("索引:" +indexName + " 不存在");

        return result;

    }



    private static void createIndex(String indexName) throws IOException {
        // TODO Auto-generated method stub
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        client.indices().create(request);
        System.out.println("创建了索引："+indexName);
    }
}


