package org.spring.springboot.elastic;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by wangh on 2017/1/19 0019.
 * 链接Es索引库常用操作
 */

public  class EsClient {


    /**
     * private constr
     */
    private EsClient() {
    }


    /**
     * 查询索引 获取一个索引响应对象
     *
     * @param client
     * @return
     */
    public static GetResponse findByIndex(TransportClient client, String index_name, String index_id) {
        GetResponse response = null;
        response = client.prepareGet().setIndex(index_name).setId(index_id).get();
        return response;
    }

    /**
     * 根据索引查询记录
     *
     * @param client    链接对象
     * @param indexName 索引名称
     * @param indexType 索引类型
     * @param indexId   索引id
     * @return
     */
    public static GetResponse findByIndex(TransportClient client, String indexName, String indexType, String indexId) {
        GetResponse response = null;
        response = client.prepareGet(indexName, indexType, indexId).get();
        return response;
    }

    /**
     * 根据索引查询记录
     *
     * @param client    链接对象
     * @param indexName 索引名称
     * @param indexType 索引类型
     * @param indexId   索引id
     * @param isAsync   是否运行在同一线程上执行
     * @return
     */
    public static GetResponse findByIndex(TransportClient client, String indexName, String indexType, String indexId, boolean isAsync) {
        GetResponse response = null;
        response = client.prepareGet(indexName, indexType, indexId).setOperationThreaded(isAsync).get();
        return response;
    }

    /**
     * 删除索引 删除一个索引响应对象
     *
     * @param client
     * @return
     */
  /*  public static DeleteResponse delByIndex(TransportClient client) {
        DeleteResponse response = null;
        response = client.prepareDelete().get();
        return response;
    }*/

    /**
     * 根据索引删除记录
     *
     * @param client    链接对象
     * @param indexName 索引名称
     * @param indexType 索引类型
     * @param indexId   索引id
     * @return
     */
    public static DeleteResponse delByIndex(TransportClient client, String indexName, String indexType, String indexId) {
        DeleteResponse response = null;
        //clear content
        response = client.prepareDelete(indexName, indexType, indexId).get();
        return response;
    }

    /**
     * 根据索引删除记录
     *
     * @param client    链接对象
     * @param indexName 索引名称
     * @param indexType 索引类型
     * @param indexId   索引id
     * @param isAsync   是否运行在同一线程上执行
     * @return
     */
    public static DeleteResponse delByIndex(TransportClient client, String indexName, String indexType, String indexId, boolean isAsync) {
        DeleteResponse response = null;
        response = client.prepareDelete(indexName, indexType, indexId)//.setOperationThreaded(isAsync)  5.2 支持
                .get();
        return response;
    }

    public static String delIndex(TransportClient client, String indexName){
        String ok ="OK";
        try{
            client.admin().indices().prepareDelete(indexName).get();
        }catch (Exception e){
            ok="FAIL";
        }
        return ok;
    }
    /**
     * 根据索引修改记录
     *
     * @param client    链接对象
     * @param xb        修改文档参数，查找定位条件
     * @param indexName 索引名称
     * @param indexType 索引类型
     * @param indexId   索引id
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static UpdateResponse updateByIndex(TransportClient client, XContentBuilder xb, String indexName, String indexType, String indexId) throws ExecutionException, InterruptedException {

        UpdateResponse response = null;
        UpdateRequest request = new UpdateRequest();
        request.index(indexName);
        request.type(indexType);
        request.id(indexId);
        request.doc(xb);

        return response = client.update(request).get();
    }

    /**
     * 根据索引修改记录
     *
     * @param client    链接对象
     * @param map       修改文档参数，查找定位条件
     * @param indexName 索引名称
     * @param indexType 索引类型
     * @param indexId   索引id
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static UpdateResponse updateByIndex(TransportClient client, Map map, String indexName, String indexType, String indexId) throws ExecutionException, InterruptedException, IOException {

        UpdateResponse response = null;
        UpdateRequest request = new UpdateRequest();
        request.index(indexName);
        request.type(indexType);
        request.id(indexId);
        request.doc(EsDataHelper.map2XcontentBuilder(map));

        return response = client.update(request).get();
    }

    /**
     * 根据索引修改记录
     *
     * @param client    链接对象
     * @param bytes       修改文档参数，查找定位条件
     * @param indexName 索引名称
     * @param indexType 索引类型
     * @param indexId   索引id
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static UpdateResponse updateByIndex(TransportClient client, byte [] bytes, String indexName, String indexType, String indexId) throws ExecutionException, InterruptedException, IOException {

        UpdateResponse response = null;
        UpdateRequest request = new UpdateRequest();
        request.index(indexName);
        request.type(indexType);
        request.id(indexId);
        request.doc(bytes);

        return response = client.update(request).get();
    }

    /**
     * 根据索引修改记录
     *
     * @param client    链接对象
     * @param xb        修改文档参数，查找定位条件
     * @param indexName 索引名称
     * @param indexType 索引类型
     * @param indexId   索引id
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static UpdateResponse updateByIndex2(TransportClient client, XContentBuilder xb, String indexName, String indexType, String indexId) throws ExecutionException, InterruptedException {

        UpdateResponse response = null;
        response = client.prepareUpdate(indexName, indexType, indexId)
                .setDoc(xb).get();

        return response;
    }

    public static String batchRequestAction(TransportClient client, List<Object> reqList) {

        String info = "执行成功";
        BulkRequestBuilder bulk = client.prepareBulk();
        for (Object obj : reqList
                ) {
            if (obj instanceof IndexRequest) {
                bulk.add((IndexRequest) obj);
            } else if (obj instanceof DeleteRequest) {
                bulk.add((DeleteRequest) obj);
            } else if (obj instanceof UpdateRequest) {
                bulk.add((UpdateRequest) obj);
            } else {
                continue;
            }
        }

        BulkResponse response = bulk.get();

        if (response.hasFailures()) {
            BulkItemResponse[] items = response.getItems();
            for (BulkItemResponse item : items
                    ) {
                System.err.println(item.getFailure());
            }
            info = "执行失败，详细错误请查看打印日志！";
        }

        return info;
    }

    /**
     * 返回索引库中总记录数
     * @param client
     * @param index_name
     * @param index_type
     * @return
     */
    public Long getTotal(TransportClient client, String index_name, String index_type) {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index_name)
                .setTypes(index_type)
                .setQuery(QueryBuilders.matchAllQuery());
        SearchResponse response = searchRequestBuilder.execute().actionGet();
        return response.getHits().getTotalHits();
    }

    /**
     * 获取库中符合条件是数据数量
     * @param client
     * @param index_name
     * @param index_type
     * @param queryBuilder
     * @return
     */
    protected Long getCountBy(TransportClient client, String index_name, String index_type, QueryBuilder queryBuilder) {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index_name)
                .setTypes(index_type)
                .setQuery(queryBuilder);
        SearchResponse response = searchRequestBuilder.execute().actionGet();
        return response.getHits().getTotalHits();
    }
}
