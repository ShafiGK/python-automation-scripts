#!/usr/bin/env python3
"""
Kafka Consumer Lag Monitor

Monitors consumer lag for specified consumer groups.
Alerts when lag exceeds threshold.

Author: Shafique Khan
"""

import logging
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = ['localhost:9092']
LAG_THRESHOLD = 1000  # Alert if lag exceeds this

def get_consumer_lag(group_id):
    """Calculate lag for a consumer group across all partitions."""
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP_SERVERS, group_id=group_id)
    
    try:
        offsets = admin.list_consumer_group_offsets(group_id)
        total_lag = 0
        
        for tp, offset_meta in offsets.items():
            end_offset = consumer.end_offsets([tp])[tp]
            lag = end_offset - offset_meta.offset
            total_lag += lag
            logger.info(f"Topic: {tp.topic}, Partition: {tp.partition}, 
                       f"Lag: {lag}")
        
        return total_lag
    finally:
        consumer.close()

def main():
    """Main monitoring loop."""
    groups = ["learn-consumer-group"]
    
    for group in groups:
        try:
            lag = get_consumer_lag(group)
            if lag > LAG_THRESHOLD:
                logger.warning(f"⚠️ HIGH LAG: {group} has {lag} messages pending")
            else:
                logger.info(f"✓ {group}: {lag} messages lag (OK)")
        except Exception as e:
            logger.error(f"Error checking {group}: {e}")

if __name__ == '__main__':
    main()
