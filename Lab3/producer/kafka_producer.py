from kafka import KafkaProducer
import json
import logging

logger = logging.getLogger(__name__)

def send_to_kafka(data):
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=5000,
            retries=3
        )

        message = {
            'table': data['table'],
            'columns': data['columns'],
            'rows': data['rows'],
            'timestamp': data.get('timestamp')
        }
        
        producer.send('etl-data', value=message).get(timeout=10)
        producer.flush()
        producer.close()
        
        logger.info(f"Сообщение отправлено в Kafka: {data['table']}")
        return True, "Успешно отправлено"
        
    except Exception as e:
        logger.error(f"Ошибка отправки в Kafka: {str(e)}")
        return False, f"Ошибка Kafka: {str(e)}"
