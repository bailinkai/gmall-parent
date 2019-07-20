package com.blk.gmalllogger.controller;


import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Demo1Controller {

    @GetMapping("testDemo")
    public String testDemo(){
        return "hello demo";
    }

}
