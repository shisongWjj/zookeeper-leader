package com.ss.zookeeper.example;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * QuartzJob
 *
 * @author shisong
 * @date 2020/9/15
 */
public class QuartzJob extends QuartzJobBean {
    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        System.out.println("[QuartzJob]-----:"+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
    }
}
