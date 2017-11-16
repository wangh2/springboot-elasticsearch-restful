package org.spring.springboot.elastic;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * Created by wangh on 2017/2/10 0010.
 */
public class EsTestMain {

    //test data
    static Map<String,Object> testData = new HashMap<String,Object>();

    // for test
    public static  void main(String ... a) throws JsonProcessingException {
        initData(testData);
        String cluster_name = "hjh.elastic";
        String node_name ="node-10.1.66.55";
        String host ="10.1.66.55";
        int port =9300;
        TransportClient tc =null;
        try {
           tc = EsClientFactory.getEsClient(EsClientFactory.getEsSettings(cluster_name ,node_name),host,port);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        System.out.println("tc\t:"+tc);


       // EsIndex.deleteIndex(tc,"webmagic");
        IndexResponse d = tc.prepareIndex().setIndex("elastic-data").setType("elastic-type").setSource(testData).execute().actionGet();
      // EsIndex.deleteIndex(tc,"webmagic");


        System.out.println("GetResponse result\t:"+d.getId());
        System.out.println("GetResponse result\t:"+d.getIndex());
        System.out.println("GetResponse result\t:"+d.getType());
        System.out.println("GetResponse result\t:"+d.getVersion());

     /*   QueryBuilder qb = termQuery("multi", "indexId");
        SearchResponse scrollResp = tc.prepareSearch("aaa")
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(60000))
                .setQuery(qb)
                .setSize(100).get();

        SearchHit[] sh =scrollResp.getHits().getHits();
        for (SearchHit s:sh
             ) {
            System.out.println("GetResponse result\t:"+s.getId());
            System.out.println("GetResponse result\t:"+s.getIndex());
            System.out.println("GetResponse result\t:"+s.getSourceAsString());
            System.out.println("GetResponse result\t:"+s.getType());
            System.out.println("GetResponse result\t:"+s.getVersion());
        }
*/

        SearchRequestBuilder searchRequestBuilder = tc.prepareSearch("elastic-data")
                .setTypes("elastic-type")
                .setQuery(QueryBuilders.matchAllQuery());
        SearchResponse response = searchRequestBuilder.execute().actionGet();
        if (response.getHits().getTotalHits() != 0) {
            try{
                SearchHit[] hits  = response.getHits().getHits();
                for (int i =0;i<hits.length;i++){
                    System.out.println(hits[i].getIndex());
                    System.out.println(hits[i].getFields());
                    System.out.println(hits[i].getId());
                    System.out.println(hits[i].getSourceAsString());

                }

            } catch (Exception e) {
                System.out.println("索引 Webpage 出错," + e.getLocalizedMessage());
            }
        }


    }

    //add data
    private static void initData(Map<String, Object> testData) {

        testData.put("123" ,"11111");
        testData.put("234" ,"va22222l2");
        testData.put("345" ,"3333");
        testData.put("456" ,"444444");
    }

    //wait TODO........

    /***********************************************************************************/

    public static void searchTelmitel() throws UnknownHostException {
        QueryBuilder qb = termQuery("multi", "test");
        TransportClient transportClient =EsClientFactory.getEsClient(new HashMap());
        SearchResponse scrollResp = transportClient.prepareSearch("test")
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(60000))
                .setQuery(qb)
                .setSize(100).get(); //max of 100 hits will be returned for each scroll
//Scroll until no hits are returned
        do {
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                //Handle the hit...
            }

            scrollResp = transportClient.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
        } while(scrollResp.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop

        //  if (domain.size() == 0) return Lists.newArrayList();
       /* BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        domain.forEach(s -> boolQueryBuilder.should(QueryBuilders.matchQuery("domain", s)));
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(INDEX_NAME)
                .setTypes(TYPE_NAME)
                .setQuery(boolQueryBuilder)
                .addSort("gatherTime", SortOrder.DESC)
                .setSize(size).setFrom(size * (page - 1));
        SearchResponse response = searchRequestBuilder.execute().actionGet();*/
        //
        // .

    }


}
