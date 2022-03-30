import io
import json
from fdk import response 
from kafka import KafkaConsumer
from confluent_kafka import Consumer
import certifi
import traceback
import logging
import time

# Based on the following documentatioN: 
# Pre-requisites: https://docs.oracle.com/en/cloud/paas/integration-cloud/stream-service-adapter/prerequisites-creating-connection.html
# Code      : https://docs.oracle.com/en-us/iaas/Content/Streaming/Tasks/streaming-kafka-python-client-quickstart.htm 
# Code: https://blogs.oracle.com/developers/post/integrating-oracle-functions-and-oracle-streaming-service

__REGION='sa-santiago-1'
__STREAM_NAME='sample_stream'
__TENANCY_NAME='ecrcloud'
__STREAMPOOL_OCID='ocid1.streampool.oc1.sa-santiago-1.amaaaaaatwfhi7yann7kjkdd4ftxo6xjzutflmqvb7za3xovrm4nc6qwmxia'
__OCI_USERNAME='oracleidentitycloudservice/denny.alquinta@oracle.com'
__SASL_USERNAME=__TENANCY_NAME+'/'+__OCI_USERNAME+'/'+__STREAMPOOL_OCID
__SASL_TOKEN='T_YUSe3W7VIbMui2kbj:'



def handler(ctx, data: io.BytesIO = None):       
    resp = oci_consumer(__STREAM_NAME, __SASL_USERNAME, __SASL_TOKEN, __REGION)
    return response.Response(
            ctx,
            response_data=json.dumps(resp),
            headers={"Content-Type": "application/json"}
    )      


def oci_consumer(stream_name, sasl_username, sasl_token, region):
    before=time.time()
    topic = stream_name
    conf = {  
        'bootstrap.servers': 'cell-1.streaming.'+region+'.oci.oraclecloud.com', 
        'security.protocol': 'SASL_SSL',       
        'ssl.ca.location': certifi.where(),   
        'sasl.mechanism': 'PLAIN',
        'sasl.username': sasl_username,  
        'sasl.password': sasl_token,  
        'group.id':'group-0',
        'api.version.request': False,
        'session.timeout.ms': 6000,
    }  

    # Create Consumer instance
    consumer = Consumer(conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    try:        
        msg = consumer.poll(0.5)
        if msg is None:
            # No message available within timeout.
            # Initial message consumption may take up to
            # `session.timeout.ms` for the consumer group to
            # rebalance and start consuming
            print("Waiting for message or event/error in poll()")       
            response = { "No new messages": "OK", "Time": time.time()-before }     
            
        else:
            # Check for Kafka message
            record_key = "Null" if msg.key() is None else msg.key().decode('utf-8')
            record_value = msg.value().decode('utf-8')         
            returnable_value = "Consumed record with key "+ record_key + " and value " + record_value
            response = { returnable_value: "OK", "Time": time.time()-before}
            consumer.close()
            
    except Exception as e:
        response = {"Error: "+str(e): "FAULT"}
        logging.exception(e, exc_info=True)
        consumer.close()
    return response
    
        
        


