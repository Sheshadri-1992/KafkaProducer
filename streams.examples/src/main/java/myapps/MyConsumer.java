/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package myapps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * In this example, we implement a simple LineSplit program using the high-level Streams DSL
 * that reads from a source topic "streams-plaintext-input", where the values of messages represent lines of text,
 * and writes the messages as-is into a sink topic "streams-pipe-output".
 */
public class MyConsumer {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-consumer");
        props.put("bootstrap.servers", "localhost:9092");    
        props.put("group.id","test-consumer-group");
        props.put("enable.auto.commit", "true");
     	props.put("auto.commit.interval.ms", "1000");
     	props.put("auto.offset.reset","earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        System.out.println("Consumer process");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);        
	    consumer.subscribe(Arrays.asList("streams-plaintext-input"));

	    consumer.poll(0);
        consumer.seekToBeginning(consumer.assignment());
	    int i=0;
	    
    	i = i+1;
    	System.out.println("I am here "+i);
        ConsumerRecords<String, String> records = consumer.poll(1000);
        System.out.println("The size of records is "+records.isEmpty());
        System.out.println("The count of records is "+records.count());
        for (ConsumerRecord<String, String> record : records){
        	System.out.println("shesh");
            System.out.println("offset = %d, key = %s, value = %s%n"+ record.offset()+" " + record.key()+" " +record.value());
        }
	    

	}

}