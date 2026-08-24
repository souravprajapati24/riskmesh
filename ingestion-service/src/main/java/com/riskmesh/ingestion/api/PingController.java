package com.riskmesh.ingestion.api;

import com.riskmesh.common.web.ValidationException;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PingController {

    @GetMapping("/internal/ping")
    public Map<String, String> ping() {
        return Map.of("service", "ingestion-service", "status", "UP");
    }

    @GetMapping("/internal/ping/error")
    public Map<String, String> pingError() {
        throw new ValidationException(
                "scaffolding error demo", List.of("This is a demo validation error"));
    }
}
