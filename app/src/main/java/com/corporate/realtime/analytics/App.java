/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package com.corporate.realtime.analytics;

import java.io.PrintStream;
import java.util.Properties;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class App {

    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public String getGreeting() {
        return "Hello World!";
    }

    public static void main(String[] args) throws Exception {
        System.out.println(new App().getGreeting());

		final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> text = env.readTextFile(params.get("Input"));

        DataSet<String> filtered = text.filter(new FilterFunction<String>()
        {
            public boolean filter(String value){
                return value.startsWith("N");
            }
        });

        DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());

        DataSet<Tuple2<String,Integer>> counts = tokenized.groupBy(0).sum(1);

        if(params.has("Output")){
            counts.writeAsCsv("Output","\n"," ");
            env.execute("WorkCount Example");
        }
    }

    public static final class Tokenizer implements MapFunction<String, Tuple2<String,Integer>>
    {
        @Override
        public Tuple2<String, Integer> map(String value){
            return new Tuple2<String, Integer>(value, 1);
        }
    }

}
