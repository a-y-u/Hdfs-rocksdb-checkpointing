package com.datatorrent.example.operators;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonConverterDuplicate extends BaseOperator
{
  private static final ObjectMapper mapper = new ObjectMapper();
  private static transient Logger logger = LoggerFactory.getLogger(JsonConverterDuplicate.class);
  public transient final DefaultOutputPort<String> output = new DefaultOutputPort<>();
  public transient final DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object o)
    {
      try {
        output.emit(mapper.writeValueAsString(o));
        logger.info("inside json operator");
      } catch (JsonProcessingException e) {
      }
    }
  };
}

