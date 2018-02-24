package com.datatorrent.example.operators;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonConverterUnique extends BaseOperator
{
    private static final ObjectMapper mapper = new ObjectMapper();
    private static transient Logger logger = LoggerFactory.getLogger(JsonConverterDuplicate.class);
    public transient final DefaultOutputPort<String> output = new DefaultOutputPort<>();
    public transient final DefaultInputPort<String> input = new DefaultInputPort<String>()
    {
        @Override
        public void process(String s)
        {
                output.emit(s);
                logger.info("inside json operator");

        }
    };
}

