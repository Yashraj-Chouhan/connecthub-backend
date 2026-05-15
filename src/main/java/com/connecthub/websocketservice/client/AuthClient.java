package com.connecthub.websocketservice.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Map;

@FeignClient(name = "auth-service", path = "/auth")
public interface AuthClient {

    @PutMapping("/users/{userId}/status")
    Map<String, Object> updateStatus(@PathVariable("userId") String userId,
                                     @RequestParam String status);
}
