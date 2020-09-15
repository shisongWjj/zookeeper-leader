package com.ss.zookeeper.example;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * ZkSchedulerFactoryBean
 *
 * @author shisong
 * @date 2020/9/15
 */
public class ZkSchedulerFactoryBean extends SchedulerFactoryBean {

    private CuratorFramework client;

    private LeaderLatch leaderLatch;

    public ZkSchedulerFactoryBean() throws Exception {
        leaderLatch = new LeaderLatch(getClient(),"/leader");
        super.setAutoStartup(false);
        leaderLatch.addListener(new ZkLeaderLatchListener(getIp(),this));
        leaderLatch.start();
    }

    private CuratorFramework getClient() {
        client = CuratorFrameworkFactory
                .builder()
                .connectString("10.0.3.84:2181")
                .retryPolicy(new ExponentialBackoffRetry(1000,3))
                .build();
        client.start();
        return client;
    }

    private String getIp(){
        String host=null;
        try {
            host= InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return host;
    }

    @Override
    protected void startScheduler(Scheduler scheduler, int startupDelay) throws SchedulerException {
        if(this.isAutoStartup()){
            //如果是自动启动的，则直接执行任务
            //原定时任务，这里默认是true
            //那么我这里肯定不行，初始化的时候 要让它设为false
            //只有当当前节点变为leader节点，才将它变为true
            super.startScheduler(scheduler, startupDelay);
        }
    }

    @Override
    public void destroy() throws SchedulerException {
        CloseableUtils.closeQuietly(leaderLatch);
        super.destroy();
    }
}
