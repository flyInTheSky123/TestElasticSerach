package com.person.demo4;

import com.person.demo3.pojo.Product;
import com.person.demo3.util.ProductUtil;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.List;
import java.util.Map;


//使用模糊查询
public class TestElastisearch {

    private static RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost("localhost", 9200, "http")
            ));
    private static String indexName = "how2java";

    public static void main(String[] args) throws IOException {

        String keyword = "时尚连衣裙";
        int start = 0;
        int count = 10;

        SearchHits hits = search(keyword, start, count);

        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {

            System.out.println(hit.getSourceAsString());
        }

        client.close();

    }

    private static SearchHits search(String keyword, int start, int count) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //关键字匹配
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("name",keyword );
        //模糊匹配
        matchQueryBuilder.fuzziness(Fuzziness.AUTO);
        sourceBuilder.query(matchQueryBuilder);
        //第几页
        sourceBuilder.from(start);
        //第几条
        sourceBuilder.size(count);

        searchRequest.source(sourceBuilder);
        //匹配度从高到低
        sourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));

        SearchResponse searchResponse = client.search(searchRequest);

        SearchHits hits = searchResponse.getHits();
        return hits;
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
