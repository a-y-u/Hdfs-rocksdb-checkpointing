package com.datatorrent.example.operators;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public class JsonConverter extends BaseOperator
{
  private static final ObjectMapper mapper = new ObjectMapper();

  public transient final DefaultOutputPort<String> output = new DefaultOutputPort<>();
  public transient final DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object o)
    {
      try {
        output.emit(mapper.writeValueAsString(o));
      } catch (JsonProcessingException e) {
      }
    }
  };
}

