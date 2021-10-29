package com.doodle;

import com.google.common.util.concurrent.AbstractScheduledService;

import java.util.concurrent.TimeUnit;

public class ScheduledExecutor extends AbstractScheduledService {

    @Override
    protected void runOneIteration() throws Exception {
        System.out.println("Executing....");
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(0, 3, TimeUnit.SECONDS);
    }

    @Override
    protected void startUp() {
        System.out.println("StartUp Activity....");
    }


    @Override
    protected void shutDown() {
        System.out.println("Shutdown Activity...");
    }

    public static void main(String[] args) throws InterruptedException {
        ScheduledExecutor se = new ScheduledExecutor();
        se.startAsync();
        Thread.sleep(15000);
        se.stopAsync();
    }
}
