package mj.mysqlagent.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;
@JsonInclude(JsonInclude.Include.NON_NULL)
public record AgentEvent(
        long tenantId,
        long instanceId,
        String agentId,
        long ts,
        long seq,
        String type,
        Map<String, Object> payload
) {
}