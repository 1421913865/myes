package com.example.myes;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class MyesApplication {

    public static void main(String[] args) {
        SpringApplication.run(MyesApplication.class, args);
    }

}
