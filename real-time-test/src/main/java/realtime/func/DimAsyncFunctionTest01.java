package realtime.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import realtime.common.GmallConfig;
import realtime.utils.ThreadPoolUtilTest01;

import java.sql.Connection;
import java.sql.DriverManager;
import java.text.ParseException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunctionTest01<T> extends RichAsyncFunction<T, T> {

    //Phoenix连接
    private Connection connection;

    //线程池
    private ThreadPoolExecutor threadPoolExecutor;

    //要查询的表名
    private String table;

    public DimAsyncFunctionTest01(String table) {
        this.table = table;
    }

    //获取Phoenix连接，开辟线程池
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        threadPoolExecutor = ThreadPoolUtilTest01.getThreadPoolExecutor();
    }

    //根据输入的事实数据，获取要查询的主键
    public abstract String getKey(T input);

    //关联维度信息
    public abstract void join(T input, JSONObject dimInfo) throws ParseException;

    //异步查询
    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {

        threadPoolExecutor.execute(new Runnable() {
            @Override
            public void run() {

                //根据输入的事实数据，获取主键
                String key = getKey(input);

                try {

                    //查询维度信息
                    JSONObject dimInfo = DimUtilTest01.getDimInfo(connection, table, key);

                    //关联维度信息
                    join(input, dimInfo);

                    //将关联好的数据写出
                    resultFuture.complete(Collections.singleton(input));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }


    //超时
    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut:" + input);
    }
}
