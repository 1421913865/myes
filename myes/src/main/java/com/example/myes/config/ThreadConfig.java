package com.example.myes.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadPoolExecutor;

//@Slf4j
@Component
public class ThreadConfig {

	private static final Logger log = LoggerFactory.getLogger(ThreadConfig.class);

	/**
	 * 线程池工具类
	 *
	 * @return {@link ThreadPoolTaskExecutor}
	 */
	@Bean("threadPoolExecutor")
	public ThreadPoolTaskExecutor asyncExecutor() {
		final int cpuSize = Runtime.getRuntime().availableProcessors();
		final int maxPoolSize=cpuSize+1;
		final int queueCapacity=cpuSize;
		log.info("最小线程个数:{};最小线程个数:{};队列最大数目:{}",(int)cpuSize/2,maxPoolSize,queueCapacity);
		log.info("初始化线程池.....");
		ThreadPoolTaskExecutor executor = new VisiableThreadPoolTaskExecutor();
		//1: 核心线程数目
		executor.setCorePoolSize(cpuSize);
		//2: 指定最大线程数,只有在缓冲队列满了之后才会申请超过核心线程数的线程
		executor.setMaxPoolSize(maxPoolSize);
		//3: 队列中最大的数目
		executor.setQueueCapacity(queueCapacity);
		//5:当pool已经达到max size的时候，如何处理新任务
		// CallerRunsPolicy: 会在execute 方法的调用线程中运行被拒绝的任务,如果执行程序已关闭，则会丢弃该任务
		// AbortPolicy: 抛出java.util.concurrent.RejectedExecutionException异常
		// DiscardOldestPolicy: 抛弃旧的任务
		// DiscardPolicy: 抛弃当前的任务
		executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		//6: 线程空闲后的最大存活时间(默认值 60),当超过了核心线程出之外的线程在空闲时间到达之后会被销毁
		executor.setKeepAliveSeconds(500);

		//7:线程空闲时间,当线程空闲时间达到keepAliveSeconds(秒)时,线程会退出,直到线程数量等于corePoolSize,如果allowCoreThreadTimeout=true,则会直到线程数量等于0
		executor.setAllowCoreThreadTimeOut(false);
		executor.initialize();
		return executor;
	}

}
