package com.example.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.SortedMap;

/**
 * @author xyh
 * @date 2021/1/21 21:34
 */

@RestController
@RequestMapping("/spring-boot-demo")
public class TestController {

    @RequestMapping("/hello")
    public String hello(){
        return "hello111222";
    }

    String s = "helloworld";


}
