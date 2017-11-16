package org.spring.springboot.elastic;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * Created by wangh on 2017/2/10 0010.
 */
public class EsClientFactory {

    /**
     * 链接客服端，一个节点可以加载所有插件
     */
    private static TransportClient transportClient;

    /**
     * 链接客服端配置信息
     */
    private static Settings setting;


    /**
     * 返回链接客服端配置信息对象
     *
     * @param clusterName     es服务器配置的集群服务名称
     * @param clusterNodeName es服务器配置的集群本机节点名称
     * @return 配置信息对象
     */
    public static Settings getEsSettings(String clusterName, String clusterNodeName) {
        setting = Settings.builder()
                .put("node.name",clusterNodeName)
                .put("cluster.name",clusterName).build();
        return setting;
    }

    /**
     * 使用es嗅探功能，默认会自动嗅探主节点还是数据节点，推荐正常集群环境下使用此配置
     *
     * @return
     */
    public static Settings getEsSettings() {
        setting = Settings.builder().put("client.transport.sniff", true).build();
        return setting;
    }

    /**
     * 获取安装有xpack认真插件的设置
     * @param clusterName
     * @param clusterNodeName
     * @return
     */
    public static Settings getEsSettingsXpack(String clusterName, String clusterNodeName) {
        setting = Settings.builder().put("cluster.name",clusterName)
                .put("node.name",clusterNodeName)
                .put("xpack.security.transport.ssl.enabled" ,false)
                .put("xpack.security.user" ,"elastic:wangh2")
                .put("client.transport.sniff" , true)
                .build();
        return setting;
    }

    /**
     * 获取一个连接客服端，此链接对象不使用es集群，使用一个或多个es服务器轮询操作
     *
     * @param hostAndPort 一个或多个 es服务器主机和端口
     * @return es服务器连接操作节点
     * @throws UnknownHostException 无法找到主机地址
     */
    public static TransportClient getEsClient(Map<String, Integer> hostAndPort) throws UnknownHostException {

        //Non Null judgment
        if (null == hostAndPort || hostAndPort.containsKey("") || hostAndPort.containsValue("")) {
            return null;
        }
        transportClient = new PreBuiltTransportClient(Settings.EMPTY);
        for (Map.Entry<String, Integer> entry : hostAndPort.entrySet()
                ) {
            transportClient.
                    addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(entry.getKey()), entry.getValue()));
        }

        return transportClient;
    }

    /**
     * 获取一个连接es服务器的客服端对象，此链接使用集群配置信息
     *
     * @param settings 集群配置信息对象
     * @param host     主机地址
     * @param port     主机端口
     * @return es服务器连接操作节点
     */
    public static TransportClient getEsClient(Settings settings, String host, Integer port) throws UnknownHostException {
        transportClient = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
        return transportClient;
    }
}
