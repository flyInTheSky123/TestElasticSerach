package com.person.demo3;

import com.person.demo3.pojo.Product;
import com.person.demo3.util.ProductUtil;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;


//通过bulkRequest()对象 进行批量地将txt文本中的数据上传到ES（ElasticSearch）中。
public class TestElastisearch {

    private static RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost("localhost", 9200, "http")
            )
    );

    //索引为：commodity 商品。
    private static String indexName = "cmd";

    public static void main(String[] args) throws IOException {

        //判断该indexName是否存在
        if (!checkIndexExits(indexName)) {
            //不存在时，创建
            createIndex(indexName);
        }

        //读取该文件里面的内容。
        List<Product> products = ProductUtil.file2list("140k_products.txt");
        System.out.println("该文件有" + products.size() + "条数据");
        batchInsert(products);
        client.close();


    }

    //使用bulkRequest() 上传到ES。
    public static void batchInsert(List<Product> products) throws IOException {
        //创建bulkRequest()
        BulkRequest bulkRequest = new BulkRequest();
        for (Product p : products) {
            //将product转换为 map 。
            Map<String, Object> map = p.toMap();
            IndexRequest product = new IndexRequest(indexName, "product", String.valueOf(p.getId())).source(map);
            bulkRequest.add(product);
        }
        client.bulk(bulkRequest);
    }

    //判断该indexname索引， 是否存在。
    private static boolean checkIndexExits(String indexName) throws IOException {
        boolean flag = true;

        try {
            //如果不存在，则会报错，进入catch{ } 中
            OpenIndexRequest index = new OpenIndexRequest(indexName);
            client.indices().open(index).isAcknowledged();
        }catch (ElasticsearchStatusException e) {
            //不存在时，就设置为false.
            flag = false;

        }
        if (flag) {
            System.out.println("该 indexName :" + indexName + "存在！");
        } else {
            System.out.println("该 indexName :" + indexName + "不存在 ！");
        }

        return flag;
    }

    //当indexname 不存在时，就创建
    public static void createIndex(String indexName) throws IOException {
        //创建索引
//        try {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
            client.indices().create(createIndexRequest);
//        } catch (IOException e) {
//            System.out.println("创建索引失败 !!");
//            e.printStackTrace();
//        }
    }


}
