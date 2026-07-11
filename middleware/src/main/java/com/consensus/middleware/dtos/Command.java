package com.consensus.middleware.dtos;

import lombok.Data;

@Data
public class Command {
    public String key;
    public String value;
    public String type;
}
