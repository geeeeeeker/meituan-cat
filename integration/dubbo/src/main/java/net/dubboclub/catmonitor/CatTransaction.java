package net.dubboclub.catmonitor;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.Activate;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.TimeoutException;
import com.alibaba.dubbo.rpc.*;
import com.alibaba.dubbo.rpc.support.RpcUtils;
import com.dianping.cat.Cat;
import com.dianping.cat.log.CatLogger;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;
import net.dubboclub.catmonitor.constants.CatConstants;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by bieber on 2015/11/4.
 */
@Activate(group = {Constants.PROVIDER, Constants.CONSUMER},order = -9000)
public class CatTransaction implements Filter {

    private final static String DUBBO_BIZ_ERROR="DUBBO_BIZ_ERROR";

    private final static String DUBBO_TIMEOUT_ERROR="DUBBO_TIMEOUT_ERROR";
    
    private final static String DUBBO_REMOTING_ERROR="DUBBO_REMOTING_ERROR";

    /** 线程上下文隔离Cat.Context */
    private static final ThreadLocal<Cat.Context> CAT_CONTEXT = new ThreadLocal<Cat.Context>();

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {

        //DubboCat未开启，绕过埋点
        if(!DubboCat.isEnable()){
            Result result =  invoker.invoke(invocation);
            return result;
        }
        URL url = invoker.getUrl();
        String sideKey = url.getParameter(Constants.SIDE_KEY);
        String loggerName = invoker.getInterface().getSimpleName()+"."+invocation.getMethodName();
        String type = CatConstants.CROSS_CONSUMER;
        if(Constants.PROVIDER_SIDE.equals(sideKey)){
            type= CatConstants.CROSS_SERVER;
        }

        boolean init = false;
        Transaction transaction = null;

        try{
            transaction = Cat.newTransaction(type /* PigeonCall或PigeonService */, loggerName /* 简单类名.方法名 */);
            Cat.Context context = getContext();
            if(Constants.CONSUMER_SIDE.equals(sideKey)){
                createConsumerCross(url,transaction);
                Cat.logRemoteCallClient(context);
            }else{
                createProviderCross(url,transaction);
                Cat.logRemoteCallServer(context);
            }
            setAttachment(context);
            init = true;
        } catch (Exception e) {
            CatLogger.getInstance().error("[DUBBO] DubboCat init error.", e);
        }

        Result result=null;
        try{
            result =  invoker.invoke(invocation);

            if (!init) {
                return result;
            }

            boolean isAsync = RpcUtils.isAsync(invoker.getUrl(), invocation);

            //异步的不能判断是否有异常,这样会阻塞住接口(<AsyncRpcResult>hasException->getRpcResult->resultFuture.get()
            if (isAsync) {
                transaction.setStatus(Message.SUCCESS);
                return result;
            }

            if(result.hasException()){
                //给调用接口出现异常进行打点
                Throwable throwable = result.getException();
                Event event = null;
                if(RpcException.class==throwable.getClass()){
                    Throwable caseBy = throwable.getCause();
                    if(caseBy!=null&&caseBy.getClass()==TimeoutException.class){
                        event = Cat.newEvent(DUBBO_TIMEOUT_ERROR,loggerName);
                    }else{
                        event = Cat.newEvent(DUBBO_REMOTING_ERROR,loggerName);
                    }
                }else if(RemotingException.class.isAssignableFrom(throwable.getClass())){
                    event = Cat.newEvent(DUBBO_REMOTING_ERROR,loggerName);
                }else{
                    event = Cat.newEvent(DUBBO_BIZ_ERROR,loggerName);
                }
                event.setStatus(result.getException());
                completeEvent(event);
                transaction.addChild(event);
                transaction.setStatus(result.getException().getClass().getSimpleName());
            }else{
                transaction.setStatus(Message.SUCCESS);
            }
            return result;
        }catch (RuntimeException e){
            if (init) {
                Cat.logError(e);
                Event event;
                if (RpcException.class == e.getClass()) {
                    Throwable caseBy = e.getCause();
                    if (caseBy != null && caseBy.getClass() == TimeoutException.class) {
                        event = Cat.newEvent(DUBBO_TIMEOUT_ERROR, loggerName);
                    } else {
                        event = Cat.newEvent(DUBBO_REMOTING_ERROR, loggerName);
                    }
                } else {
                    event = Cat.newEvent(DUBBO_BIZ_ERROR, loggerName);
                }
                event.setStatus(e);
                completeEvent(event);
                transaction.addChild(event);
                transaction.setStatus(e.getClass().getSimpleName());
            }
            if(result==null){
                throw e;
            }else{
                return result;
            }
        }finally {
            if (transaction != null) {
                transaction.complete();
            }
            CAT_CONTEXT.remove();
        }
    }

    static class DubboCatContext implements Cat.Context{

        private Map<String,String> properties = new HashMap<String, String>();

        @Override
        public void addProperty(String key, String value) {
            properties.put(key,value);
        }

        @Override
        public String getProperty(String key) {
            return properties.get(key);
        }
    }

    private String getProviderAppName(URL url){
        String appName = url.getParameter(CatConstants.PROVIDER_APPLICATION_NAME);
        if(StringUtils.isEmpty(appName)){
            String interfaceName  = url.getParameter(Constants.INTERFACE_KEY);
            appName = interfaceName.substring(0,interfaceName.lastIndexOf('.'));
        }
        return appName;
    }

    private void setAttachment(Cat.Context context){
        RpcContext.getContext().setAttachment(Cat.Context.ROOT,context.getProperty(Cat.Context.ROOT));
        RpcContext.getContext().setAttachment(Cat.Context.CHILD,context.getProperty(Cat.Context.CHILD));
        RpcContext.getContext().setAttachment(Cat.Context.PARENT,context.getProperty(Cat.Context.PARENT));
    }

    private Cat.Context getContext(){
        Cat.Context context = CAT_CONTEXT.get();
        if(context==null){
            context = initContext();
            CAT_CONTEXT.set(context);
        }
        return context;
    }

    private Cat.Context initContext(){
        Cat.Context context = new DubboCatContext();
        Map<String,String> attachments = RpcContext.getContext().getAttachments();
        if(attachments!=null&&attachments.size()>0){
            for(Map.Entry<String,String> entry:attachments.entrySet()){
                if(Cat.Context.CHILD.equals(entry.getKey())||Cat.Context.ROOT.equals(entry.getKey())||Cat.Context.PARENT.equals(entry.getKey())){
                    context.addProperty(entry.getKey(),entry.getValue());
                }
            }
        }
        return context;
    }

    /**
     * 创建消费者跨端埋点
     *
     * @param url
     * @param transaction
     */
    private void createConsumerCross(URL url,Transaction transaction){
        Event crossAppEvent =   Cat.newEvent(CatConstants.CONSUMER_CALL_APP,getProviderAppName(url));
        Event crossServerEvent =   Cat.newEvent(CatConstants.CONSUMER_CALL_SERVER,url.getHost());
        Event crossPortEvent =   Cat.newEvent(CatConstants.CONSUMER_CALL_PORT,url.getPort()+"");
        crossAppEvent.setStatus(Event.SUCCESS);
        crossServerEvent.setStatus(Event.SUCCESS);
        crossPortEvent.setStatus(Event.SUCCESS);
        completeEvent(crossAppEvent);
        completeEvent(crossPortEvent);
        completeEvent(crossServerEvent);
        transaction.addChild(crossAppEvent);
        transaction.addChild(crossPortEvent);
        transaction.addChild(crossServerEvent);
    }

    private void completeEvent(Event event){
        event.complete();
    }

    /**
     * 创建生产者跨端埋点
     *
     * @param url
     * @param transaction
     */
    private void createProviderCross(URL url,Transaction transaction){
        String consumerAppName = RpcContext.getContext().getAttachment(Constants.APPLICATION_KEY);
        if(StringUtils.isEmpty(consumerAppName)){
            consumerAppName= RpcContext.getContext().getRemoteHost()+":"+ RpcContext.getContext().getRemotePort();
        }
        Event crossAppEvent = Cat.newEvent(CatConstants.PROVIDER_CALL_APP,consumerAppName);
        Event crossServerEvent = Cat.newEvent(CatConstants.PROVIDER_CALL_SERVER, RpcContext.getContext().getRemoteHost());
        crossAppEvent.setStatus(Event.SUCCESS);
        crossServerEvent.setStatus(Event.SUCCESS);
        completeEvent(crossAppEvent);
        completeEvent(crossServerEvent);
        transaction.addChild(crossAppEvent);
        transaction.addChild(crossServerEvent);
    }
}
