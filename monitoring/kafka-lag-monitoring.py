#!/usr/bin/env python3

import logging
from kafka import KafkaAdminClient, KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s"
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = ["localhost:9092"]
LAG_THRESHOLD = 10


def list_all_groups(admin):
    groups = admin.list_consumer_groups()
    return [group[0] for group in groups]


def get_group_status(admin, consumer, group_id):
    total_lag = 0
    active_members = 0
    group_state = "UNKNOWN"

    try:
        desc = admin.describe_consumer_groups([group_id])
        if desc:
            group_info = desc[0]
            active_members = len(group_info.members or [])
            group_state = getattr(group_info, "state", "UNKNOWN")
    except Exception as e:
        logger.warning(f"Could not describe group {group_id}: {e}")

    try:
        offsets = admin.list_consumer_group_offsets(group_id)
    except Exception as e:
        logger.warning(f"Could not fetch offsets for {group_id}: {e}")
        return {
            "group_id": group_id,
            "state": group_state,
            "active_members": active_members,
            "total_lag": 0,
        }

    for tp, offset_meta in offsets.items():
        if offset_meta.offset is None or offset_meta.offset < 0:
            logger.warning(
                f"No committed offset for group={group_id}, "
                f"topic={tp.topic}, partition={tp.partition}"
            )
            continue

        try:
            end_offset = consumer.end_offsets([tp])[tp]
            lag = end_offset - offset_meta.offset
            total_lag += lag

            logger.info(
                f"Group={group_id}, Topic={tp.topic}, "
                f"Partition={tp.partition}, Lag={lag}"
            )
        except Exception as e:
            logger.warning(
                f"Failed lag check for group={group_id}, "
                f"topic={tp.topic}, partition={tp.partition}: {e}"
            )

    return {
        "group_id": group_id,
        "state": group_state,
        "active_members": active_members,
        "total_lag": total_lag,
    }


def monitor_groups_once():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP_SERVERS)

    try:
        logger.info("Starting one-time consumer group lag scan...")

        groups = list_all_groups(admin)

        if not groups:
            logger.info("No consumer groups found")
            return

        for group_id in groups:
            try:
                status = get_group_status(admin, consumer, group_id)

                logger.info(
                    f"SUMMARY Group={status['group_id']}, "
                    f"State={status['state']}, "
                    f"ActiveMembers={status['active_members']}, "
                    f"TotalLag={status['total_lag']}"
                )

                if status["active_members"] == 0:
                    logger.warning(f"NO ACTIVE CONSUMERS: group={group_id}")

                if status["total_lag"] > LAG_THRESHOLD:
                    logger.warning(
                        f"HIGH LAG: group={group_id}, "
                        f"lag={status['total_lag']}"
                    )

            except Exception as e:
                logger.error(f"Error checking group {group_id}: {e}")

    finally:
        consumer.close()
        admin.close()


if __name__ == "__main__":
    monitor_groups_once()
