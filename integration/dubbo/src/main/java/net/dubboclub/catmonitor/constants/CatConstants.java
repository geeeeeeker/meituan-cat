package net.dubboclub.catmonitor.constants;

/**
 * Created by bieber on 2015/11/12.
 */
public class CatConstants {

    /** 跨系统调用消费者（客户端） */
    public  static final String CROSS_CONSUMER ="PigeonCall";

    /** 跨系统调用生产者（服务端） */
    public static final String CROSS_SERVER = "PigeonService";
    
    public static final String PROVIDER_APPLICATION_NAME="serverApplicationName";

    /** 远程调用服务消费者 */
    public static final String CONSUMER_CALL_SERVER="PigeonCall.server";
    
    public static final String CONSUMER_CALL_APP="PigeonCall.app";
    
    public static final String CONSUMER_CALL_PORT="PigeonCall.port";

    /** 远程调用服务提供者 */
    public static final String PROVIDER_CALL_SERVER="PigeonService.client";

    /** 远程调用消费者应用名称 */
    public static final String PROVIDER_CALL_APP="PigeonService.app";

    public static final String FORK_MESSAGE_ID="m_forkedMessageId";

    public static final String FORK_ROOT_MESSAGE_ID="m_rootMessageId";

    public static final String FORK_PARENT_MESSAGE_ID="m_parentMessageId";
    
}
