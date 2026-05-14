import json
import logging
from datetime import datetime
from confluent_kafka import Producer

logger = logging.getLogger(__name__)

# Topic naming convention from design doc
TOPIC_MAP = {
    "Contact": "glynac.salesforce.contact.raw",
    "Account": "glynac.salesforce.account.raw",
    "Opportunity": "glynac.salesforce.opportunity.raw",
    "Task": "glynac.salesforce.task.raw",
    "Lead": "glynac.salesforce.lead.raw",
    "User": "glynac.salesforce.user.raw",
    "CampaignMember": "glynac.salesforce.campaignmember.raw"
}


class KafkaProducer:
    """
    Publishes scan events and extracted records to Kafka.
    """

    def __init__(self, settings):
        self.enabled = settings.KAFKA_ENABLED

        if self.enabled:
            self.producer = Producer({
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "client.id": "black-diamond-salesforce-service",
                "acks": "all",
                "retries": 3
            })
            logger.info(f"Kafka producer connected — {settings.KAFKA_BOOTSTRAP_SERVERS}")
        else:
            self.producer = None
            logger.info("Kafka disabled")

    def _delivery_report(self, err, msg):
        """Callback fired when message is delivered or fails."""
        if err:
            logger.error(f"Kafka delivery failed — topic={msg.topic()} error={err}")
        else:
            logger.debug(f"Kafka delivered — topic={msg.topic()} partition={msg.partition()}")

    def publish_scan_event(self, scan_id: str, org_id: str, event_type: str, data: dict = None):
        """
        Publishes scan lifecycle events.
        event_type: scan_started | scan_complete | scan_failed
        """
        if not self.enabled:
            logger.info(f"Kafka disabled — skipping event {event_type}")
            return

        topic = "glynac.salesforce.scan.events"
        message = {
            "event_type": event_type,
            "scan_id": scan_id,
            "org_id": org_id,
            "timestamp": datetime.utcnow().isoformat(),
            "data": data or {}
        }

        try:
            self.producer.produce(
                topic,
                key=scan_id,
                value=json.dumps(message),
                callback=self._delivery_report
            )
            self.producer.poll(0)
            logger.info(f"Kafka event published — type={event_type} scan_id={scan_id}")

        except Exception as e:
            logger.error(f"Kafka publish failed — {str(e)}")
            raise

    def publish_records(self, sf_object: str, records: list, scan_id: str, org_id: str):
        """
        Publishes extracted records to the object-specific topic.
        Java equivalent: kafkaTemplate.send(topic, key, value)
        """
        if not self.enabled:
            logger.info(f"Kafka disabled — skipping {len(records)} records")
            return

        topic = TOPIC_MAP.get(sf_object, f"glynac.salesforce.{sf_object.lower()}.raw")

        for record in records:
            message = {
                "scan_id": scan_id,
                "org_id": org_id,
                "sf_object": sf_object,
                "timestamp": datetime.utcnow().isoformat(),
                "record": record
            }
            try:
                self.producer.produce(
                    topic,
                    key=record.get("Id", scan_id),
                    value=json.dumps(message),
                    callback=self._delivery_report
                )
            except Exception as e:
                logger.error(f"Kafka record publish failed — {str(e)}")
                raise

        # Flush all messages
        self.producer.flush()
        logger.info(f"Kafka published {len(records)} records — object={sf_object} topic={topic}")

    def flush(self):
        """Force flush any pending messages."""
        if self.enabled and self.producer:
            self.producer.flush()