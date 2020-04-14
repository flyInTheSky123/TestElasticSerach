package com.person;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
//使用客户端操作elasticsearch
// 判断/创建/删除索引
public class TestElasticsearch {
    //通过 RestHighLevelClient 查看索引是否存在
    private static RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost("localhost", 9200, "http")
            ));

    public static void main(String[] args) throws IOException {

        String indexName = "how";

        if(!checkExistIndex(indexName)){
            createIndex(indexName);
        }

        if(checkExistIndex(indexName)){
            deleteIndex(indexName);
        }
        checkExistIndex(indexName);
        client.close();
    }

    //判断索引是否存在
    private static boolean checkExistIndex(String indexName) throws IOException {
        boolean result =true;
        try {

            OpenIndexRequest openIndexRequest = new OpenIndexRequest(indexName);
            client.indices().open(openIndexRequest).isAcknowledged();

        } catch (ElasticsearchStatusException ex) {
//            String m = "Elasticsearch exception [type=index_not_found_exception, reason=no such index]";
//            if (m.equals(ex.getMessage())) {
                result = false;
          //  }
        }finally {
            if(result) {
                System.out.println("索引:" + indexName + " 是存在的");
            }
            else{
                System.out.println("索引:" +indexName + " 不存在");}

        }


        return result;

    }


    //删除索引
    private static void deleteIndex(String indexName) throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        client.indices().delete(request);
        System.out.println("删除了索引："+indexName);

    }

    //创建索引
    private static void createIndex(String indexName) throws IOException {
        // TODO Auto-generated method stub
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        client.indices().create(request);
        System.out.println("创建了索引："+indexName);
    }

}


