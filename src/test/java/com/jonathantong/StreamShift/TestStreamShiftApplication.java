package com.jonathantong.StreamShift;

import org.springframework.boot.SpringApplication;

public class TestStreamShiftApplication {

	public static void main(String[] args) {
		SpringApplication.from(StreamShiftApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
