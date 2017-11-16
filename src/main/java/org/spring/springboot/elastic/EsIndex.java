package org.spring.springboot.elastic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Created by wangh on 2017/2/10 0010.
 */
public class EsIndex {

    /**
     * object 2 str
     */
    public static final ObjectMapper mapper = new ObjectMapper();


    /**
     * 创建索引 索引名称，类型，id自动分配
     *
     * @param client  链接对象
     * @param strJson 索引对应的内容
     * @return 非负数则创建成功
     */
    public static long createIndex(TransportClient client, Object strJson) {

        long result = 1;
        try {
            IndexRequestBuilder indexBuilder = client.prepareIndex();
            IndexResponse indexResponse = writeContent(indexBuilder, strJson);
            result = indexResponse.getVersion();
        } catch (Exception e) {
            result = -1;
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 创建索引
     *
     * @param client    链接对象
     * @param strJson   索引对应的内容
     * @param indexName 索引名称
     * @param indexType 索引类型
     * @return 非负数则创建成功
     */
    public static long createIndex(TransportClient client, Object strJson, String indexName, String indexType) {

        long result = 1;
        IndexResponse indexResponse = null;
        try {
            IndexRequestBuilder indexBuilder = client.prepareIndex(indexName, indexType);
            if(null==strJson){
                indexResponse = indexBuilder.setSource().get();
            }else {
                indexResponse = writeContent(indexBuilder, strJson);
            }
            result = indexResponse.getVersion();
        } catch (Exception e) {
            result = -1;
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 创建索引
     *
     * @param client    链接对象
     * @param strJson   索引对应的内容 只创建索引 可以为null
     * @param indexName 索引名称
     * @param indexType 索引类型
     * @param indexId   索引id
     * @return 非负数则创建成功
     */
    public static long createIndex(TransportClient client, Object strJson, String indexName, String indexType, String indexId) {

        long result = 1l;
        IndexResponse indexResponse = null;
        try {

            IndexRequestBuilder indexBuilder = client.prepareIndex(indexName, indexType, indexId);
            if(null==strJson){
                indexResponse = indexBuilder.setSource().get();
            }else {
                indexResponse = writeContent(indexBuilder, strJson);
            }
            result = indexResponse.getVersion();
        } catch (Exception e) {
            result = -1;
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 写入指定格式的索引内容
     *
     * @param indexBuilder 索引引用地址
     * @param data     内容
     */
    private static IndexResponse writeContent(IndexRequestBuilder indexBuilder, Object data) throws IOException {
        IndexResponse indexResponse = null;

            if (data instanceof String) {
                indexResponse = indexBuilder.setSource((String)data).get();
            } else if (data instanceof Map) {
                indexResponse = indexBuilder.setSource((Map<String, ?>) data).get();
            } else if (data instanceof byte[]) {
                indexResponse = indexBuilder.setSource((byte[]) data).get();
            } else if (data instanceof XContentBuilder) {
                indexResponse = indexBuilder.setSource((XContentBuilder)data).get();
            } else {
                try {
                    String json = mapper.writeValueAsString(data);
                    indexBuilder.setSource(json).get();
                } catch (Exception e) {
                    e.printStackTrace();
                    ;
                    throw new RuntimeException("创建索引失败，可能原因 ：数据格式不正确！期望类型： javaBean,String,Map,byte[],XContentBuilder!");
                }
        }
        return indexResponse;
    }

    /**
     * 删除一个索引库，不可逆操作，谨慎调用
     *
     * @param client 链接操作对象
     * @param index  索引库名称
     */
    public static void deleteIndex(TransportClient client, String index) {
        client.admin().indices().prepareDelete(index).get();
    }

    /**
     * 删除多个索引库，不可逆操作，谨慎掉用
     *
     * @param client 链接操作对象
     * @param index  索引库名称
     */
    public static void deleteIndexs(TransportClient client, String... index) {
        for (String indexName : index
                ) {
            deleteIndex(client, indexName);
        }
    }
}
