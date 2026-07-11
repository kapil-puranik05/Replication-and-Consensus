package com.consensus.middleware.routing;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

@Data
public class NodeDetails {
    private List<String> registry = new ArrayList<>();
    private String leaderAddress = "";
}
