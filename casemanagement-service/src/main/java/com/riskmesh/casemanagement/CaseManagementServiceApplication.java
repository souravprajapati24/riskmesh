package com.riskmesh.casemanagement;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.riskmesh")
public class CaseManagementServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(CaseManagementServiceApplication.class, args);
    }
}
