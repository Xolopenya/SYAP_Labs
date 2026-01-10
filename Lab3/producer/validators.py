import json
import re
from datetime import datetime

def validate_data(data):
    if not data:
        return "Нет данных"
    
    if not data.get('table'):
        return "Название таблицы не указано"
    
    if not data.get('columns'):
        return "Столбцы не указаны"
    
    if not data.get('rows'):
        return "Данные не указаны"
    
    if not isinstance(data['rows'], list):
        return "Данные должны быть массивом"
    
    if len(data['rows']) == 0:
        return "Данные пусты"
    
    return None

def validate_table_structure(table_name, columns):
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
        return "Неверное название таблицы (только буквы, цифры и подчёркивание)"
    
    if len(table_name) > 63:
        return "Название таблицы слишком длинное"

    if not isinstance(columns, list):
        return "Столбцы должны быть массивом"
    
    if len(columns) == 0:
        return "Нет столбцов"
    
    valid_types = {'INTEGER', 'TEXT', 'NUMERIC', 'DATE', 'TIMESTAMP', 'BOOLEAN'}
    
    for col in columns:
        if not col.get('name'):
            return "Название столбца не указано"
        
        if not col.get('type'):
            return f"Тип столбца '{col['name']}' не указан"
        
        if col['type'] not in valid_types:
            return f"Неверный тип '{col['type']}' для столбца '{col['name']}'"
        
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', col['name']):
            return f"Неверное название столбца '{col['name']}'"
    
    return None

def validate_row_data(row, columns):
    for col in columns:
        col_name = col['name']
        col_type = col['type']
        
        if col_name not in row:
            return f"Столбец '{col_name}' отсутствует в строке"
        
        value = row[col_name]
        
        if col_type == 'INTEGER':
            if value is not None and not isinstance(value, int):
                try:
                    int(value)
                except:
                    return f"'{col_name}' должно быть целым числом, получено: {value}"
        
        elif col_type == 'NUMERIC':
            if value is not None:
                try:
                    float(value)
                except:
                    return f"'{col_name}' должно быть числом, получено: {value}"
        
        elif col_type == 'DATE':
            if value is not None:
                try:
                    datetime.strptime(str(value), '%Y-%m-%d')
                except:
                    return f"'{col_name}' должна быть датой (YYYY-MM-DD), получено: {value}"
        
        elif col_type == 'TIMESTAMP':
            if value is not None:
                try:
                    datetime.fromisoformat(str(value))
                except:
                    return f"'{col_name}' должна быть временной меткой (ISO format), получено: {value}"
        
        elif col_type == 'BOOLEAN':
            if value is not None and not isinstance(value, bool):
                return f"'{col_name}' должно быть булевым, получено: {value}"
    
    return None
