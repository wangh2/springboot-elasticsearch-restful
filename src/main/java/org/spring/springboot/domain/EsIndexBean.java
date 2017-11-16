package org.spring.springboot.domain;

/**
 * @author wh
 * @description write...
 * @create 2017-11-14 下午 1:41
 **/
public class EsIndexBean {
    String indexName;
    String indexType;
    String indexId;

    public EsIndexBean() {
    }

    public String getIndexName() {
        return this.indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexType() {
        return this.indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public String getIndexId() {
        return this.indexId;
    }

    public void setIndexId(String indexId) {
        this.indexId = indexId;
    }
}
