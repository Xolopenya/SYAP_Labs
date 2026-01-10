from kafka import KafkaConsumer
import json
import logging
from db_handler import create_table, insert_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def start_consumer():
    try:
        logger.info(" Подключение к БД: postgres@localhost:etl_db")
        
        consumer = KafkaConsumer(
            'etl-data',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='etl-consumer-group',
            auto_offset_reset='earliest',
            max_poll_records=1,
            session_timeout_ms=30000,
            request_timeout_ms=40000
        )
        
        logger.info(" Consumer подключен к Kafka: localhost:9092")
        logger.info(" Слушаем топик: etl-data")
        logger.info(" ETL Consumer запущен")
        logger.info(" Ожидание сообщений из Kafka...")
        
        for message in consumer:
            try:
                data = message.value
                
                logger.info(f"   Получено сообщение:")
                logger.info(f"   Таблица: '{data['table']}'")
                logger.info(f"   Столбцов: {len(data['columns'])}")
                logger.info(f"   Строк: {len(data['rows'])}")

                create_success, create_msg = create_table(
                    data['table'],
                    data['columns']
                )
                
                if not create_success:
                    logger.error(f" Ошибка создания таблицы: {create_msg}")
                    continue
                
                logger.info(f" Таблица '{data['table']}' создана")

                insert_success, insert_msg = insert_data(
                    data['table'],
                    data['columns'],
                    data['rows']
                )
                
                if insert_success:
                    logger.info(f" Вставлено {insert_msg} строк в таблицу '{data['table']}'")
                else:
                    logger.error(f" Ошибка вставки данных: {insert_msg}")
                
            except Exception as e:
                logger.error(f" Ошибка обработки сообщения: {str(e)}")
                continue
    
    except Exception as e:
        logger.error(f" Ошибка Consumer'а: {str(e)}")
        raise

if __name__ == '__main__':
    start_consumer()
