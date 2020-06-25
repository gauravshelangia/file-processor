package com.company;

import java.util.concurrent.*;

public class ExecutorFactory {

    // this should be moved to a Factory
    public static ExecutorService getExecutorService(int nThreads){
        ArrayBlockingQueue<Runnable> blockingQueue = new ArrayBlockingQueue<Runnable>(2*nThreads);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(nThreads, nThreads, 0L,
                TimeUnit.MILLISECONDS, blockingQueue);
        executor.setRejectedExecutionHandler(new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                if (!executor.isShutdown()){
                    try {
                        executor.getQueue().put(r);
                    } catch (InterruptedException e) {
                        System.out.println("Queue not accepting new request. Something wrong !!!");
                        e.printStackTrace();
                    }
                }
            }
        });
        return executor;
    }
}
