package com.ss.zookeeper.example;

import org.quartz.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

/**
 * Quartz
 *
 * @author shisong
 * @date 2020/9/14
 */
@Configuration
public class QuartzConfig {

    @Bean
    public SchedulerFactoryBean schedulerFactoryBean() throws Exception {
        SchedulerFactoryBean schedulerFactoryBean = new ZkSchedulerFactoryBean();
        //执行哪个job
        schedulerFactoryBean.setJobDetails(jobDetail());
        //job执行的规律
        schedulerFactoryBean.setTriggers(trigger());
        return schedulerFactoryBean;
    }

    @Bean
    public JobDetail jobDetail(){
        return JobBuilder.newJob(QuartzJob.class).storeDurably().build();
    }

    @Bean
    public Trigger trigger(){
        SimpleScheduleBuilder simpleScheduleBuilder = SimpleScheduleBuilder
                .simpleSchedule()
                .withIntervalInSeconds(1)
                .repeatForever();

        return TriggerBuilder
                .newTrigger()
                .forJob(jobDetail())
                .withSchedule(simpleScheduleBuilder)
                .build();
    }
}
