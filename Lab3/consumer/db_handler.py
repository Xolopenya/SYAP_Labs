import psycopg2
from psycopg2 import sql
import logging

logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'etl_db',
    'user': 'postgres',
    'password': 'password'
}

def get_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f" Ошибка подключения к БД: {str(e)}")
        return None

def create_table(table_name, columns):
    try:
        conn = get_connection()
        if not conn:
            return False, "Нет подключения к БД"
        
        cursor = conn.cursor()
        
        # Построение SQL команды
        col_definitions = []
        for col in columns:
            col_name = col['name']
            col_type = col['type']
            
            # Преобразование типов
            if col_type == 'INTEGER':
                sql_type = 'INTEGER'
            elif col_type == 'NUMERIC':
                sql_type = 'NUMERIC'
            elif col_type == 'TEXT':
                sql_type = 'TEXT'
            elif col_type == 'DATE':
                sql_type = 'DATE'
            elif col_type == 'TIMESTAMP':
                sql_type = 'TIMESTAMP'
            elif col_type == 'BOOLEAN':
                sql_type = 'BOOLEAN'
            else:
                sql_type = 'TEXT'
            
            col_definitions.append(f"{col_name} {sql_type}")
        
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_definitions)})"
        
        cursor.execute(create_sql)
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f" Таблица '{table_name}' создана/уже существует")
        return True, "Таблица создана"
        
    except Exception as e:
        logger.error(f" Ошибка создания таблицы: {str(e)}")
        return False, str(e)

def insert_data(table_name, columns, rows):
    try:
        conn = get_connection()
        if not conn:
            return False, "Нет подключения к БД"
        
        cursor = conn.cursor()
        
        # Подготовка SQL команды
        col_names = [col['name'] for col in columns]
        placeholders = ', '.join(['%s'] * len(col_names))
        insert_sql = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({placeholders})"

        inserted = 0
        for row in rows:
            try:
                values = [row.get(col_name) for col_name in col_names]
                cursor.execute(insert_sql, values)
                inserted += 1
            except Exception as e:
                logger.error(f" Ошибка вставки строки: {str(e)}")
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f" Вставлено {inserted} строк в '{table_name}'")
        return True, inserted
        
    except Exception as e:
        logger.error(f" Ошибка вставки данных: {str(e)}")
        return False, str(e)
