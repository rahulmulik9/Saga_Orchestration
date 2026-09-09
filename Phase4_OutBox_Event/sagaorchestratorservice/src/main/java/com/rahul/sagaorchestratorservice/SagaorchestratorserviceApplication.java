package com.rahul.sagaorchestratorservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class SagaorchestratorserviceApplication {

	public static void main(String[] args) {
		SpringApplication.run(SagaorchestratorserviceApplication.class, args);
	}

}
