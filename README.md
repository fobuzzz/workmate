# CSV Processor

Скрипт для обработки CSV-файлов с поддержкой фильтрации, агрегации и сортировки данных.

## Возможности

- **Фильтрация** с операторами: `>`, `<`, `=`, `>=`, `<=`, `!=`
- **Агрегация** с функциями: `avg` (среднее), `min` (минимум), `max` (максимум), `median` (медиана), `sum` (сумма), `count` (количество)
- **Сортировка** с направлениями: `asc` (по возрастанию), `desc` (по убыванию)
- Поддержка как числовых, так и текстовых данных
- Красивое отображение результатов в виде таблиц
- **Расширенная обработка ошибок** с информативными сообщениями
- **Валидация входных данных** на всех этапах обработки

## Установка

1. Клонируйте репозиторий:
```bash
git clone <repository-url>
cd python_example
```

2. Установите зависимости:
```bash
pip install -r requirements.txt
```

## Использование

### Базовый синтаксис

```bash
python csv_processor.py <файл.csv> [--where "условие"] [--aggregate "колонка=функция"] [--order-by "колонка=направление"]
```

### Примеры использования

#### Просмотр всех данных
```bash
python csv_processor.py sample_data.csv
```

#### Фильтрация по цене (больше 500)
```bash
python csv_processor.py sample_data.csv --where "price>500"
```

#### Фильтрация по бренду
```bash
python csv_processor.py sample_data.csv --where "brand=apple"
```

#### Вычисление средней цены
```bash
python csv_processor.py sample_data.csv --aggregate "price=avg"
```

#### Поиск максимальной цены
```bash
python csv_processor.py sample_data.csv --aggregate "price=max"
```

#### Вычисление медианы цен
```bash
python csv_processor.py sample_data.csv --aggregate "price=median"
```

#### Подсчет количества товаров
```bash
python csv_processor.py sample_data.csv --aggregate "price=count"
```

#### Сортировка по цене (по убыванию)
```bash
python csv_processor.py sample_data.csv --order-by "price=desc"
```

#### Сортировка по названию (по возрастанию)
```bash
python csv_processor.py sample_data.csv --order-by "name=asc"
```

#### Комбинирование фильтрации и сортировки
```bash
python csv_processor.py sample_data.csv --where "price>500" --order-by "rating=desc"
```

### Поддерживаемые операторы фильтрации

- `>` - больше
- `<` - меньше
- `=` - равно
- `>=` - больше или равно
- `<=` - меньше или равно
- `!=` - не равно

### Поддерживаемые функции агрегации

- `avg` - среднее значение
- `min` - минимальное значение
- `max` - максимальное значение
- `median` - медиана
- `sum` - сумма
- `count` - количество непустых значений

### Поддерживаемые направления сортировки

- `asc` - по возрастанию
- `desc` - по убыванию

## Обработка ошибок

Скрипт предоставляет подробную информацию об ошибках:

### Примеры ошибок и их решения

```bash
# Несуществующий файл
python csv_processor.py nonexistent.csv
# Ошибка: Файл не найден: nonexistent.csv

# Неверный формат условия фильтрации
python csv_processor.py sample_data.csv --where "price>"
# Ошибка валидации фильтрации: Значение для сравнения не может быть пустым

# Несуществующая колонка
python csv_processor.py sample_data.csv --where "nonexistent>500"
# Ошибка валидации фильтрации: Колонка 'nonexistent' не найдена. Доступные колонки: name, brand, price, rating

# Неподдерживаемый тип агрегации
python csv_processor.py sample_data.csv --aggregate "price=invalid"
# Ошибка валидации агрегации: Неподдерживаемый тип агрегации: invalid. Поддерживаемые: avg, min, max, median, sum, count

# Неверное направление сортировки
python csv_processor.py sample_data.csv --order-by "price=invalid"
# Ошибка валидации сортировки: Неверное направление сортировки: invalid. Используйте 'asc' или 'desc'
```

## Тестирование

Запуск тестов:
```bash
pytest test_csv_processor.py -v
```

Запуск тестов с покрытием:
```bash
pytest test_csv_processor.py --cov=csv_processor --cov-report=term-missing
```

## Архитектура

Проект построен с использованием объектно-ориентированного подхода и паттернов проектирования:

### Основные компоненты

- `CSVProcessor` - основной класс для обработки CSV-файлов
- `FilterCondition` - класс для представления условий фильтрации
- `Aggregator` - абстрактный базовый класс для агрегаторов
- `AggregatorFactory` - фабрика для создания агрегаторов
- `Sorter` - класс для сортировки данных
- `ValidationError` - класс для ошибок валидации

### Паттерны проектирования

1. **Стратегия (Strategy)** - для агрегаторов и сортировки
2. **Фабрика (Factory)** - для создания агрегаторов
3. **Команда (Command)** - для обработки различных операций

### Расширяемость

Архитектура позволяет легко добавлять новые возможности:

#### Добавление нового агрегатора
```python
class NewAggregator(Aggregator):
    def aggregate(self, values: List[Any]) -> Any:
        # Реализация новой агрегации
        pass

# Добавить в AggregatorFactory.AGGREGATORS
AGGREGATORS = {
    # ... существующие агрегаторы
    'new': NewAggregator
}
```

#### Добавление нового оператора фильтрации
```python
# Добавить в FilterCondition.OPERATORS
OPERATORS = {
    # ... существующие операторы
    'new_op': new_operator_function
}
```

## Пример CSV-файла

```csv
name,brand,price,rating
iphone 15 pro,apple,999,4.9
galaxy s23 ultra,samsung,1199,4.8
redmi note 12,xiaomi,199,4.6
poco x5 pro,xiaomi,299,4.4
pixel 8 pro,google,899,4.7
oneplus 11,oneplus,699,4.5
```

## Требования

- Python 3.7+
- tabulate
- pytest (для тестирования)
- pytest-cov (для анализа покрытия кода)

## Дополнительные возможности

### Новые агрегаторы
- **Медиана** - центральное значение в отсортированном ряду
- **Сумма** - сумма всех числовых значений
- **Количество** - подсчет непустых значений

### Сортировка
- Поддержка сортировки по любой колонке
- Автоматическое определение типа данных (числа/строки)
- Направления сортировки: по возрастанию и убыванию

### Валидация
- Проверка существования колонок
- Валидация форматов входных данных
- Информативные сообщения об ошибках
- Обработка пустых файлов и некорректных данных 