package realtime.utils;


import java.util.concurrent.*;

public class ThreadPoolUtilTest01 {

    //声明线程池
    private static ThreadPoolExecutor threadPoolExecutor = null;

    public static ThreadPoolExecutor getThreadPoolExecutor(){

        if (threadPoolExecutor == null) {

            //使用懒汉式加载，节省资源，但是有线程安全，加锁，双重判断
            synchronized (ThreadPoolUtilTest01.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(4,
                            20,
                            60,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<>());
                }
            }
        }

        return threadPoolExecutor;
    }

}
