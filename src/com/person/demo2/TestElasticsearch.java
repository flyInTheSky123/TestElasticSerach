package com.person.demo2;

import com.person.demo2.pojo.Product;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

//使用客户端操作elasticsearch的文档
// 判断/创建/删除/获取文档
public class TestElasticsearch {


    //通过 RestHighLevelClient 查看索引是否存在
    private static RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost("localhost", 9200, "http")
            ));
    //索引名称
    //******注意，这里的indexName 不能写 "elastic"   ********
    private static String indexName = "try";
  //  private static String _type = "product";


    public static void main(String[] args) throws IOException {

//确保索引存在
        if (!checkExistIndex(indexName)) {
            createIndex(indexName);
        }
        //数据
        Product product = new Product();
        product.setId(1);
        product.setName("李薏");


        //添加文档
        addDocument(product);

        //查看文档
        //getDocument(int id,String a);
        getDocument(1);

        //修改数据
        product.setName("product 2");
        //修改文档
        updateDocument(product);
        //获取文档
        getDocument(1);

        //删除文档
        deleteDocument(1);
        //获取文档
        getDocument(1);

        client.close();


    }
    private static void deleteDocument(int id) throws IOException {
        DeleteRequest  deleteRequest = new DeleteRequest(indexName,"product", String.valueOf(id));
        client.delete(deleteRequest);
        System.out.println("已经从ElasticSearch服务器上删除id="+id+"的文档");
    }

    private static void updateDocument(Product product) throws IOException {

        UpdateRequest updateRequest = new UpdateRequest (indexName, "product", String.valueOf(product.getId()))
                .doc("name",product.getName());

        client.update(updateRequest);
        System.out.println("已经在ElasticSearch服务器修改产品为："+product);

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


    //判断索引是否存在
    private static boolean checkExistIndex(String indexName) throws IOException {
        boolean result = true;
        try {

            OpenIndexRequest openIndexRequest = new OpenIndexRequest(indexName);
            client.indices().open(openIndexRequest).isAcknowledged();

        } catch (ElasticsearchStatusException ex) {
            String m = "Elasticsearch exception [type=index_not_found_exception, reason=no such index]";
            if (m.equals(ex.getMessage())) {
            result = false;
              }
        }
            if (result) {
                System.out.println("索引:" + indexName + " 是存在的");
            } else {
                System.out.println("索引:" + indexName + " 不存在");
        }


        return result;

    }


    //创建索引
    private static void createIndex(String indexName) throws IOException {
        // TODO Auto-generated method stub
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        client.indices().create(request);
        System.out.println("创建了索引：" + indexName);
    }

}


