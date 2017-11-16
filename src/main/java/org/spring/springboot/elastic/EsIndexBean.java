package org.spring.springboot.elastic;

/**
 * Created by wangh on 2017/1/20 0020.
 */
public class EsIndexBean {

    String indexName ;
    String indexType;
    String indexId;

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public String getIndexId() {
        return indexId;
    }

    public void setIndexId(String indexId) {
        this.indexId = indexId;
    }
}
