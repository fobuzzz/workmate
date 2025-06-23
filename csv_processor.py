# CSV Processor - модуль для обработки CSV-файлов с поддержкой фильтрации и агрегации


import csv
import argparse
import sys
from typing import List, Dict, Any, Optional, Union
from tabulate import tabulate
import operator
from abc import ABC, abstractmethod


class CSVProcessorError(Exception):
    # Базовый класс для ошибок CSV процессора
    pass


class ValidationError(CSVProcessorError):
    # Ошибка валидации входных данных
    pass


class FilterCondition:
    # Класс для представления условия фильтрации

    OPERATORS = {
        '>': operator.gt,
        '<': operator.lt,
        '=': operator.eq,
        '>=': operator.ge,
        '<=': operator.le,
        '!=': operator.ne
    }

    def __init__(self, column: str, operator: str, value: Any):
        self.column = column
        self.operator = operator
        self.value = value

        # Валидация
        if not column.strip():
            raise ValidationError("Название колонки не может быть пустым")
        if operator not in self.OPERATORS:
            raise ValidationError(f"Неподдерживаемый оператор: {operator}")

    def evaluate(self, row: Dict[str, Any]) -> bool:
        # Проверяет, удовлетворяет ли строка условию фильтрации
        if self.column not in row:
            return False

        cell_value = row[self.column]

        # Попытка преобразовать в число для числового сравнения
        try:
            cell_value = float(cell_value)
            compare_value = float(self.value)
        except (ValueError, TypeError):
            # Если не удалось преобразовать в число, сравниваем как строки
            cell_value = str(cell_value)
            compare_value = str(self.value)

        op_func = self.OPERATORS.get(self.operator)
        if op_func is None:
            raise ValueError(f"Неподдерживаемый оператор: {self.operator}")

        return op_func(cell_value, compare_value)


class Aggregator(ABC):
    # Абстрактный базовый класс для агрегаторов
    
    @abstractmethod
    def aggregate(self, values: List[Any]) -> Any:
        # Выполняет агрегацию списка значений
        pass


class AvgAggregator(Aggregator):
    # Агрегатор для вычисления среднего значения
    
    def aggregate(self, values: List[Any]) -> float:
        # Вычисляет среднее значение
        numeric_values = []
        for value in values:
            try:
                numeric_values.append(float(value))
            except (ValueError, TypeError):
                continue
                
        if not numeric_values:
            return 0.0
            
        return sum(numeric_values) / len(numeric_values)


class MinAggregator(Aggregator):
    # Агрегатор для вычисления минимального значения
    
    def aggregate(self, values: List[Any]) -> float:
        # Вычисляет минимальное значение
        numeric_values = []
        for value in values:
            try:
                numeric_values.append(float(value))
            except (ValueError, TypeError):
                continue
                
        if not numeric_values:
            return 0.0
            
        return min(numeric_values)


class MaxAggregator(Aggregator):
    # Агрегатор для вычисления максимального значения
    
    def aggregate(self, values: List[Any]) -> float:
        # Вычисляет максимальное значение
        numeric_values = []
        for value in values:
            try:
                numeric_values.append(float(value))
            except (ValueError, TypeError):
                continue
                
        if not numeric_values:
            return 0.0
            
        return max(numeric_values)


class MedianAggregator(Aggregator):
    # Агрегатор для вычисления медианы
    
    def aggregate(self, values: List[Any]) -> float:
        # Вычисляет медиану
        numeric_values = []
        for value in values:
            try:
                numeric_values.append(float(value))
            except (ValueError, TypeError):
                continue
                
        if not numeric_values:
            return 0.0
            
        numeric_values.sort()
        n = len(numeric_values)
        if n % 2 == 0:
            return (numeric_values[n//2 - 1] + numeric_values[n//2]) / 2
        else:
            return numeric_values[n//2]


class SumAggregator(Aggregator):
    # Агрегатор для вычисления суммы
    
    def aggregate(self, values: List[Any]) -> float:
        # Вычисляет сумму
        numeric_values = []
        for value in values:
            try:
                numeric_values.append(float(value))
            except (ValueError, TypeError):
                continue
                
        return sum(numeric_values)


class CountAggregator(Aggregator):
    # Агрегатор для подсчета количества значений
    
    def aggregate(self, values: List[Any]) -> int:
        # Подсчитывает количество непустых значений
        return len([v for v in values if v and str(v).strip()])


class AggregatorFactory:
    # Фабрика для создания агрегаторов
    
    AGGREGATORS = {
        'avg': AvgAggregator,
        'min': MinAggregator,
        'max': MaxAggregator,
        'median': MedianAggregator,
        'sum': SumAggregator,
        'count': CountAggregator
    }
    
    @classmethod
    def create(cls, aggregator_type: str) -> Aggregator:
        # Создает агрегатор указанного типа
        aggregator_class = cls.AGGREGATORS.get(aggregator_type.lower())
        if aggregator_class is None:
            supported = ', '.join(cls.AGGREGATORS.keys())
            raise ValidationError(f"Неподдерживаемый тип агрегации: {aggregator_type}. Поддерживаемые: {supported}")
        return aggregator_class()


class Sorter:
    # Класс для сортировки данных
    
    def __init__(self, column: str, direction: str = 'asc'):
        self.column = column
        self.direction = direction.lower()
        
        if self.direction not in ['asc', 'desc']:
            raise ValidationError(f"Неверное направление сортировки: {direction}. Используйте 'asc' или 'desc'")
    
    def sort(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # Сортирует данные по указанной колонке
        if not data:
            return data
            
        # Проверяем, что колонка существует
        if self.column not in data[0]:
            raise ValidationError(f"Колонка '{self.column}' не найдена в данных")
        
        def sort_key(row: Dict[str, Any]) -> Any:
            value = row.get(self.column, '')
            # Попытка преобразовать в число для правильной сортировки
            try:
                return float(value)
            except (ValueError, TypeError):
                return str(value)
        
        sorted_data = sorted(data, key=sort_key, reverse=(self.direction == 'desc'))
        return sorted_data


class CSVProcessor:
    # Основной класс для обработки CSV-файлов
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.data: List[Dict[str, Any]] = []
        self.headers: List[str] = []
        
    def load_data(self) -> None:
        # Загружает данные из CSV-файла
        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                first_line = file.readline().strip()
                if not first_line:
                    raise ValidationError("CSV файл пуст или не содержит данных")
                # Если первая строка не содержит ни одной буквы, считаем, что это не заголовки
                if not any(c.isalpha() for c in first_line):
                    raise ValidationError("CSV файл не содержит заголовков")
                # Если все значения в первой строке — числа, это не заголовки
                first_row_values = [v.strip() for v in first_line.split(',')]
                if all(v.replace('.', '', 1).isdigit() for v in first_row_values):
                    raise ValidationError("CSV файл не содержит заголовков")
                file.seek(0)
                reader = csv.DictReader(file)
                self.headers = reader.fieldnames or []
                self.data = list(reader)
                if not self.data:
                    if not self.headers:
                        raise ValidationError("CSV файл не содержит заголовков")
                    else:
                        raise ValidationError("CSV файл пуст или не содержит данных")
        except FileNotFoundError:
            raise FileNotFoundError(f"Файл не найден: {self.file_path}")
        except UnicodeDecodeError:
            raise ValidationError(f"Ошибка кодировки файла {self.file_path}. Убедитесь, что файл в UTF-8")
        except ValidationError:
            raise
        except Exception as e:
            raise Exception(f"Ошибка при чтении файла: {e}")
    
    def validate_column(self, column: str) -> None:
        # Проверяет существование колонки
        if column not in self.headers:
            available_columns = ', '.join(self.headers)
            raise ValidationError(f"Колонка '{column}' не найдена. Доступные колонки: {available_columns}")
    
    def filter_data(self, condition: FilterCondition) -> List[Dict[str, Any]]:
        # Фильтрует данные по заданному условию
        self.validate_column(condition.column)
        return [row for row in self.data if condition.evaluate(row)]
    
    def aggregate_data(self, column: str, aggregator: Aggregator) -> Dict[str, Any]:
        # Выполняет агрегацию данных по указанной колонке
        if not self.data:
            return {'result': 0.0}
        
        self.validate_column(column)
        values = [row.get(column, '') for row in self.data]
        result = aggregator.aggregate(values)
        
        return {'result': result}
    
    def sort_data(self, sorter: Sorter) -> List[Dict[str, Any]]:
        # Сортирует данные
        return sorter.sort(self.data.copy())
    
    def display_results(self, results: Union[List[Dict[str, Any]], Dict[str, Any]]) -> None:
        # Отображает результаты в виде таблицы
        if isinstance(results, list):
            if not results:
                print("Результаты не найдены.")
                return
            print(tabulate(results, headers='keys', tablefmt='grid'))
        else:
            print(tabulate([results], headers='keys', tablefmt='grid'))


def parse_condition(condition_str: str) -> FilterCondition:
    # Парсит строку условия фильтрации
    if not condition_str or not condition_str.strip():
        raise ValidationError("Условие фильтрации не может быть пустым")
    
    operators = ['>=', '<=', '!=', '>', '<', '=']
    
    for op in operators:
        if op in condition_str:
            parts = condition_str.split(op, 1)
            if len(parts) == 2:
                column = parts[0].strip()
                value = parts[1].strip()
                
                if not column:
                    raise ValidationError("Название колонки не может быть пустым")
                if not value:
                    raise ValidationError("Значение для сравнения не может быть пустым")
                    
                return FilterCondition(column, op, value)
    
    raise ValidationError(f"Неверный формат условия: {condition_str}. Пример: 'price>500'")


def parse_aggregate(aggregate_str: str) -> tuple[str, str]:
    # Парсит строку агрегации
    if not aggregate_str or not aggregate_str.strip():
        raise ValidationError("Условие агрегации не может быть пустым")
    
    if '=' not in aggregate_str:
        raise ValidationError(f"Неверный формат агрегации: {aggregate_str}. Пример: 'price=avg'")
    
    parts = aggregate_str.split('=', 1)
    if len(parts) != 2:
        raise ValidationError(f"Неверный формат агрегации: {aggregate_str}")
    
    column = parts[0].strip()
    aggregator_type = parts[1].strip()
    
    if not column:
        raise ValidationError("Название колонки не может быть пустым")
    if not aggregator_type:
        raise ValidationError("Тип агрегации не может быть пустым")
    
    return column, aggregator_type


def parse_order_by(order_by_str: str) -> Sorter:
    # Парсит строку сортировки
    if not order_by_str or not order_by_str.strip():
        raise ValidationError("Условие сортировки не может быть пустым")
    
    if '=' not in order_by_str:
        raise ValidationError(f"Неверный формат сортировки: {order_by_str}. Пример: 'price=desc'")
    
    parts = order_by_str.split('=', 1)
    if len(parts) != 2:
        raise ValidationError(f"Неверный формат сортировки: {order_by_str}")
    
    column = parts[0].strip()
    direction = parts[1].strip()
    
    if not column:
        raise ValidationError("Название колонки не может быть пустым")
    if not direction:
        raise ValidationError("Направление сортировки не может быть пустым")
    
    return Sorter(column, direction)


def print_usage_examples() -> None:
    # Выводит примеры использования
    print("\nПримеры использования:")
    print("  python csv_processor.py data.csv")
    print("  python csv_processor.py data.csv --where 'price>500'")
    print("  python csv_processor.py data.csv --where 'brand=apple'")
    print("  python csv_processor.py data.csv --aggregate 'price=avg'")
    print("  python csv_processor.py data.csv --aggregate 'price=max'")
    print("  python csv_processor.py data.csv --order-by 'price=desc'")
    print("  python csv_processor.py data.csv --where 'price>500' --order-by 'rating=desc'")


def main() -> int:
    # Основная функция программы
    parser = argparse.ArgumentParser(
        description='Обработка CSV-файлов с поддержкой фильтрации, агрегации и сортировки',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=print_usage_examples()
    )
    parser.add_argument('file', help='Путь к CSV-файлу')
    parser.add_argument('--where', help='Условие фильтрации (например: "price>500")')
    parser.add_argument('--aggregate', help='Условие агрегации (например: "price=avg")')
    parser.add_argument('--order-by', help='Условие сортировки (например: "price=desc")')
    
    try:
        args = parser.parse_args()
        
        # Проверяем, что файл указан
        if not args.file:
            print("Ошибка: Не указан файл для обработки")
            parser.print_help()
            return 1
        
        # Создаем процессор и загружаем данные
        processor = CSVProcessor(args.file)
        processor.load_data()
        
        # Обработка фильтрации
        if args.where:
            try:
                condition = parse_condition(args.where)
                filtered_data = processor.filter_data(condition)
                processor.display_results(filtered_data)
            except ValidationError as e:
                print(f"Ошибка валидации фильтрации: {e}")
                return 1
        
        # Обработка агрегации
        elif args.aggregate:
            try:
                column, aggregator_type = parse_aggregate(args.aggregate)
                aggregator = AggregatorFactory.create(aggregator_type)
                result = processor.aggregate_data(column, aggregator)
                processor.display_results(result)
            except ValidationError as e:
                print(f"Ошибка валидации агрегации: {e}")
                return 1
        
        # Если не указаны ни фильтрация, ни агрегация, показываем все данные
        else:
            data_to_display = processor.data
            
            # Применяем сортировку, если указана
            if args.order_by:
                try:
                    sorter = parse_order_by(args.order_by)
                    data_to_display = processor.sort_data(sorter)
                except ValidationError as e:
                    print(f"Ошибка валидации сортировки: {e}")
                    return 1
            
            processor.display_results(data_to_display)
            
    except FileNotFoundError as e:
        print(f"Ошибка: {e}")
        return 1
    except ValidationError as e:
        print(f"Ошибка валидации: {e}")
        return 1
    except KeyboardInterrupt:
        print("\nОперация прервана пользователем")
        return 1
    except Exception as e:
        print(f"Неожиданная ошибка: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main()) 