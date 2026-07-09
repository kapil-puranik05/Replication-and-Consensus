package com.consensus.middleware.dtos;

import lombok.Data;

@Data
public class ClusterResponse {
    private boolean accepted;
    private Integer leaderId;
}
