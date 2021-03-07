package com.pd.distributelock.controller;

import com.pd.distributelock.lock.ZkLock;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@Slf4j
@RestController
public class ZkLockController {

    @RequestMapping("zkLock")
    public String zkLock() {
        log.info("我进入了方法!");
        String connectString = "192.168.31.78:2181";
        String businessName = "order";
        try(ZkLock zkLock = new ZkLock(connectString, businessName)) {
            if(zkLock.getLock()) {
                log.info("我进入了锁!");
                Thread.sleep(15000);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        log.info("方法执行完成!");
        return "方法执行完成!";
    }

}
