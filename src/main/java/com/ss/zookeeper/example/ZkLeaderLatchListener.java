package com.ss.zookeeper.example;

import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

/**
 * ZkLeaderLatchListener
 *
 * @author shisong
 * @date 2020/9/15
 */
public class ZkLeaderLatchListener implements LeaderLatchListener {

    private static final Logger LOG= LoggerFactory.getLogger(ZkSchedulerFactoryBean.class);

    private String ip;

    private SchedulerFactoryBean schedulerFactoryBean;

    public ZkLeaderLatchListener(String ip, SchedulerFactoryBean schedulerFactoryBean) {
        this.ip = ip;
        this.schedulerFactoryBean = schedulerFactoryBean;
    }

    @Override
    public void isLeader() {
        LOG.info("ip:{} 成为leader，执行scheduler~",ip);
        //如果变为leader节点 则需要将autoStartup变为true
        //并且启动任务
        schedulerFactoryBean.setAutoStartup(true);
        schedulerFactoryBean.start();
    }

    @Override
    public void notLeader() {
        //如果不是leader节点 ，则需要将autoStartup变为false
        //并且 停止任务
        schedulerFactoryBean.setAutoStartup(false);
        schedulerFactoryBean.stop();
    }
}