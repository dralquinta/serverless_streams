import io
import json
from fdk import response 
from kafka import KafkaConsumer
import traceback
import logging

# Based on the following documentatioN: 
# Pre-requisites: https://docs.oracle.com/en/cloud/paas/integration-cloud/stream-service-adapter/prerequisites-creating-connection.html
# Code      : https://docs.oracle.com/en-us/iaas/Content/Streaming/Tasks/streaming-kafka-python-client-quickstart.htm 
# Code: https://blogs.oracle.com/developers/post/integrating-oracle-functions-and-oracle-streaming-service

__REGION='sa-santiago-1'
__STREAM_NAME='My_Stream'
__TENANCY_NAME='ecrcloud'
__STREAMPOOL_OCID='ocid1.streampool.oc1.sa-santiago-1.amaaaaaatwfhi7ya66xboit4wmfpyibkv2ekosw2foxxlsvhcqijnemmtooa'
__OCI_USERNAME='ecrcloud/oracleidentitycloudservice/denny.alquinta@oracle.com/'
__SASL_USERNAME=__TENANCY_NAME+'/'+__OCI_USERNAME+'/'+__STREAMPOOL_OCID
__SASL_TOKEN='T_YUSe3W7VIbMui2kbj:'



def handler(ctx, data: io.BytesIO = None):   
  #  resp = { "All clear here!": "OK"}
    resp = consumer(__SASL_USERNAME, __SASL_TOKEN, __REGION, __STREAM_NAME)
    return response.Response(
            ctx,
            response_data=json.dumps(resp),
            headers={"Content-Type": "application/json"}
    )      


def consumer(sasl_username, sasl_token, region, stream_name):
    try: 
        consumer = KafkaConsumer(
                            stream_name, 
                            bootstrap_servers = 'cqijnemmtooa.streaming.'+region+'.oci.oraclecloud.com:9092', 
                            security_protocol = 'SASL_SSL', sasl_mechanism = 'PLAIN',
                            consumer_timeout_ms = 10000, auto_offset_reset = 'earliest',
                            group_id='group-0',
                            sasl_plain_username = sasl_username, 
                            sasl_plain_password = sasl_token)       
        returnable_value = "Topic: "+consumer.topic+" Partition: "+consumer.partition+" Offset: "+ consumer.offset +" Key: "+consumer.key +" Value: "+consumer.value   
        response = { returnable_value: "OK"}
    except Exception as e:               
        response = {"Error: "+str(e): "FAULT"}
        logging.exception(e, exc_info=True)
    return response




