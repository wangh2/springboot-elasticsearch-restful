package org.spring.springboot.elastic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by wangh on 2017/2/10 0010.
 */
public class EsDataHelper {

    /**
     * java log
     */
    private static Logger log = Logger.getLogger("EsDataHelper");

    /**
     * object 2 str
     */
    public static final ObjectMapper mapper = new ObjectMapper();

    /**
     * private construction
     */
    /**
     * map转化es内置XcontentBuilder对象
     *
     * @param dataMap map对象
     * @return XcontentBuilder对象
     * @throws IOException
     */
    public static XContentBuilder map2XcontentBuilder(Map<String, Object> dataMap)  {

        XContentBuilder xb = null;
        try {
            xb = jsonBuilder().startObject();

            for (Map.Entry<String, Object> entry : dataMap.entrySet()
                    ) {
                xb.field(entry.getKey(), entry.getValue());
            }
            xb.endObject();
        } catch (IOException e) {
            log.info("格式转化失败，数据源 datas{ "+dataMap.toString()+"}");
            e.printStackTrace();
        }
        return xb;
    }

    /**
     * list<map>转化es内置XcontentBuilder对象
     *
     * @param dataList listmap对象
     * @return XcontentBuilder对象
     * @throws IOException
     */
    public static XContentBuilder listMap2XcontentBuilder(List<Map<String, Object>> dataList)  {

        XContentBuilder xb = null;
        try {
            xb =jsonBuilder().startObject();
            int count = dataList.size();
            while(count!=0){
                Map<String,Object> dataMap = dataList.get(count-1);
                for (Map.Entry<String, Object> entry : dataMap.entrySet()
                        ) {
                    xb.field(entry.getKey(), entry.getValue());
                }
                --count;
            }
                xb.endObject();
        } catch (IOException e) {
            log.info("格式转化失败，数据源 datas{ "+dataList.toString()+"}");
            e.printStackTrace();
        }
        return xb;
    }

    /**
     * 自定义bean转byte数组
     *
     * @param bean 自定义bean
     * @return byte[]数组
     * @throws JsonProcessingException
     */
    public static byte[] bean2Bytes(Object bean)  {
        try {
            return mapper.writeValueAsBytes(bean);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }
}
