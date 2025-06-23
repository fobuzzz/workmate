"""
Тесты для модуля csv_processor.
"""

import pytest
import tempfile
import os
from csv_processor import (
    CSVProcessor, FilterCondition, AggregatorFactory,
    AvgAggregator, MinAggregator, MaxAggregator,
    MedianAggregator, SumAggregator, CountAggregator,
    Sorter, ValidationError, CSVProcessorError,
    parse_condition, parse_aggregate, parse_order_by
)


class TestFilterCondition:
    """Тесты для класса FilterCondition."""
  #  
    def test_numeric_comparison(self):
        """Тест числового сравнения."""
        condition = FilterCondition('price', '>', '500')
        
        # Тест с числовыми значениями
        assert condition.evaluate({'price': '600'}) is True
        assert condition.evaluate({'price': '400'}) is False
        assert condition.evaluate({'price': '500'}) is False
    
    def test_string_comparison(self):
        """Тест строкового сравнения."""
        condition = FilterCondition('brand', '=', 'apple')
        
        assert condition.evaluate({'brand': 'apple'}) is True
        assert condition.evaluate({'brand': 'samsung'}) is False
    
    def test_missing_column(self):
        """Тест отсутствующей колонки."""
        condition = FilterCondition('price', '>', '500')
        assert condition.evaluate({'name': 'test'}) is False
    
    def test_invalid_operator(self):
        """Тест неверного оператора."""
        with pytest.raises(ValidationError):
            FilterCondition('price', 'invalid', '500')
    
    def test_empty_column(self):
        """Тест пустой колонки."""
        with pytest.raises(ValidationError):
            FilterCondition('', '>', '500')
    
    def test_whitespace_column(self):
        """Тест колонки только с пробелами."""
        with pytest.raises(ValidationError):
            FilterCondition('   ', '>', '500')


class TestAggregators:
    """Тесты для агрегаторов."""
    
    def test_avg_aggregator(self):
        """Тест агрегатора среднего значения."""
        aggregator = AvgAggregator()
        result = aggregator.aggregate(['1', '2', '3', '4', '5'])
        assert result == 3.0
    
    def test_avg_aggregator_with_non_numeric(self):
        """Тест агрегатора среднего значения с нечисловыми данными."""
        aggregator = AvgAggregator()
        result = aggregator.aggregate(['1', 'abc', '3', 'def', '5'])
        assert result == 3.0
    
    def test_avg_aggregator_empty(self):
        """Тест агрегатора среднего значения с пустым списком."""
        aggregator = AvgAggregator()
        result = aggregator.aggregate([])
        assert result == 0.0
    
    def test_min_aggregator(self):
        """Тест агрегатора минимального значения."""
        aggregator = MinAggregator()
        result = aggregator.aggregate(['5', '2', '8', '1', '3'])
        assert result == 1.0
    
    def test_max_aggregator(self):
        """Тест агрегатора максимального значения."""
        aggregator = MaxAggregator()
        result = aggregator.aggregate(['5', '2', '8', '1', '3'])
        assert result == 8.0
    
    def test_median_aggregator_odd(self):
        """Тест агрегатора медианы с нечетным количеством элементов."""
        aggregator = MedianAggregator()
        result = aggregator.aggregate(['1', '3', '5', '7', '9'])
        assert result == 5.0
    
    def test_median_aggregator_even(self):
        """Тест агрегатора медианы с четным количеством элементов."""
        aggregator = MedianAggregator()
        result = aggregator.aggregate(['1', '3', '5', '7'])
        assert result == 4.0  # (3 + 5) / 2
    
    def test_sum_aggregator(self):
        """Тест агрегатора суммы."""
        aggregator = SumAggregator()
        result = aggregator.aggregate(['1', '2', '3', '4', '5'])
        assert result == 15.0
    
    def test_sum_aggregator_with_non_numeric(self):
        """Тест агрегатора суммы с нечисловыми данными."""
        aggregator = SumAggregator()
        result = aggregator.aggregate(['1', 'abc', '3', 'def', '5'])
        assert result == 9.0  # 1 + 3 + 5
    
    def test_count_aggregator(self):
        """Тест агрегатора подсчета."""
        aggregator = CountAggregator()
        result = aggregator.aggregate(['1', '', '3', '   ', '5'])
        assert result == 3  # только непустые значения
    
    def test_count_aggregator_empty(self):
        """Тест агрегатора подсчета с пустым списком."""
        aggregator = CountAggregator()
        result = aggregator.aggregate([])
        assert result == 0


class TestAggregatorFactory:
    """Тесты для фабрики агрегаторов."""
    
    def test_create_avg_aggregator(self):
        """Тест создания агрегатора среднего значения."""
        aggregator = AggregatorFactory.create('avg')
        assert isinstance(aggregator, AvgAggregator)
    
    def test_create_min_aggregator(self):
        """Тест создания агрегатора минимального значения."""
        aggregator = AggregatorFactory.create('min')
        assert isinstance(aggregator, MinAggregator)
    
    def test_create_max_aggregator(self):
        """Тест создания агрегатора максимального значения."""
        aggregator = AggregatorFactory.create('max')
        assert isinstance(aggregator, MaxAggregator)
    
    def test_create_median_aggregator(self):
        """Тест создания агрегатора медианы."""
        aggregator = AggregatorFactory.create('median')
        assert isinstance(aggregator, MedianAggregator)
    
    def test_create_sum_aggregator(self):
        """Тест создания агрегатора суммы."""
        aggregator = AggregatorFactory.create('sum')
        assert isinstance(aggregator, SumAggregator)
    
    def test_create_count_aggregator(self):
        """Тест создания агрегатора подсчета."""
        aggregator = AggregatorFactory.create('count')
        assert isinstance(aggregator, CountAggregator)
    
    def test_create_invalid_aggregator(self):
        """Тест создания неверного агрегатора."""
        with pytest.raises(ValidationError) as exc_info:
            AggregatorFactory.create('invalid')
        assert "Неподдерживаемый тип агрегации" in str(exc_info.value)
        assert "avg, min, max, median, sum, count" in str(exc_info.value)


class TestSorter:
    """Тесты для класса Sorter."""
    
    def test_sort_ascending(self):
        """Тест сортировки по возрастанию."""
        sorter = Sorter('price', 'asc')
        data = [
            {'price': '300', 'name': 'c'},
            {'price': '100', 'name': 'a'},
            {'price': '200', 'name': 'b'}
        ]
        result = sorter.sort(data)
        assert [row['price'] for row in result] == ['100', '200', '300']
    
    def test_sort_descending(self):
        """Тест сортировки по убыванию."""
        sorter = Sorter('price', 'desc')
        data = [
            {'price': '100', 'name': 'a'},
            {'price': '300', 'name': 'c'},
            {'price': '200', 'name': 'b'}
        ]
        result = sorter.sort(data)
        assert [row['price'] for row in result] == ['300', '200', '100']
    
    def test_sort_string_values(self):
        """Тест сортировки строковых значений."""
        sorter = Sorter('name', 'asc')
        data = [
            {'name': 'zebra', 'price': '100'},
            {'name': 'apple', 'price': '200'},
            {'name': 'banana', 'price': '300'}
        ]
        result = sorter.sort(data)
        assert [row['name'] for row in result] == ['apple', 'banana', 'zebra']
    
    def test_sort_empty_data(self):
        """Тест сортировки пустых данных."""
        sorter = Sorter('price', 'asc')
        result = sorter.sort([])
        assert result == []
    
    def test_invalid_direction(self):
        """Тест неверного направления сортировки."""
        with pytest.raises(ValidationError):
            Sorter('price', 'invalid')
    
    def test_missing_column(self):
        """Тест сортировки по несуществующей колонке."""
        sorter = Sorter('nonexistent', 'asc')
        data = [{'price': '100'}]
        with pytest.raises(ValidationError):
            sorter.sort(data)


class TestCSVProcessor:
    """Тесты для класса CSVProcessor."""
    
    @pytest.fixture
    def sample_csv_file(self):
        """Создает временный CSV-файл для тестов."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("name,brand,price,rating\n")
            f.write("iphone 15 pro,apple,999,4.9\n")
            f.write("galaxy s23 ultra,samsung,1199,4.8\n")
            f.write("redmi note 12,xiaomi,199,4.6\n")
            f.write("poco x5 pro,xiaomi,299,4.4\n")
            f.flush()  # Убеждаемся, что данные записаны
            yield f.name
        # Удаляем файл после теста
        if os.path.exists(f.name):
            os.unlink(f.name)
    
    def test_load_data(self, sample_csv_file):
        """Тест загрузки данных из CSV-файла."""
        processor = CSVProcessor(sample_csv_file)
        processor.load_data()
        
        assert len(processor.data) == 4
        assert processor.headers == ['name', 'brand', 'price', 'rating']
        assert processor.data[0]['name'] == 'iphone 15 pro'
        assert processor.data[0]['price'] == '999'
    
    def test_load_nonexistent_file(self):
        """Тест загрузки несуществующего файла."""
        processor = CSVProcessor('nonexistent.csv')
        with pytest.raises(FileNotFoundError):
            processor.load_data()
    
    def test_load_empty_file(self):
        """Тест загрузки пустого файла."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("name,brand,price\n")  # только заголовки
            f.flush()
            
            processor = CSVProcessor(f.name)
            with pytest.raises(ValidationError) as exc_info:
                processor.load_data()
            assert "CSV файл пуст" in str(exc_info.value)
            
            os.unlink(f.name)
    
    def test_load_file_without_headers(self):
        """Тест загрузки файла без заголовков."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("iphone,apple,999\n")  # нет заголовков
            f.flush()
            
            processor = CSVProcessor(f.name)
            with pytest.raises(ValidationError) as exc_info:
                processor.load_data()
            assert "CSV файл пуст или не содержит данных" in str(exc_info.value)
            
            os.unlink(f.name)
    
    def test_filter_data(self, sample_csv_file):
        """Тест фильтрации данных."""
        processor = CSVProcessor(sample_csv_file)
        processor.load_data()
        
        condition = FilterCondition('price', '>', '500')
        filtered_data = processor.filter_data(condition)
        
        assert len(filtered_data) == 2
        assert all(int(row['price']) > 500 for row in filtered_data)
    
    def test_filter_data_invalid_column(self, sample_csv_file):
        """Тест фильтрации по несуществующей колонке."""
        processor = CSVProcessor(sample_csv_file)
        processor.load_data()
        
        condition = FilterCondition('nonexistent', '>', '500')
        with pytest.raises(ValidationError) as exc_info:
            processor.filter_data(condition)
        assert "Колонка 'nonexistent' не найдена" in str(exc_info.value)
    
    def test_aggregate_data(self, sample_csv_file):
        """Тест агрегации данных."""
        processor = CSVProcessor(sample_csv_file)
        processor.load_data()
        
        aggregator = AvgAggregator()
        result = processor.aggregate_data('price', aggregator)
        
        expected_avg = (999 + 1199 + 199 + 299) / 4
        assert abs(result['result'] - expected_avg) < 0.01
    
    def test_aggregate_data_invalid_column(self, sample_csv_file):
        """Тест агрегации по несуществующей колонке."""
        processor = CSVProcessor(sample_csv_file)
        processor.load_data()
        
        aggregator = AvgAggregator()
        with pytest.raises(ValidationError) as exc_info:
            processor.aggregate_data('nonexistent', aggregator)
        assert "Колонка 'nonexistent' не найдена" in str(exc_info.value)
    
    def test_aggregate_empty_data(self):
        """Тест агрегации пустых данных."""
        processor = CSVProcessor('dummy.csv')
        processor.data = []
        
        aggregator = AvgAggregator()
        result = processor.aggregate_data('price', aggregator)
        
        assert result['result'] == 0.0
    
    def test_sort_data(self, sample_csv_file):
        """Тест сортировки данных."""
        processor = CSVProcessor(sample_csv_file)
        processor.load_data()
        
        sorter = Sorter('price', 'desc')
        sorted_data = processor.sort_data(sorter)
        
        prices = [int(row['price']) for row in sorted_data]
        assert prices == [1199, 999, 299, 199]


class TestParsingFunctions:
    """Тесты для функций парсинга."""
    
    def test_parse_condition(self):
        """Тест парсинга условия фильтрации."""
        condition = parse_condition('price>500')
        assert condition.column == 'price'
        assert condition.operator == '>'
        assert condition.value == '500'
    
    def test_parse_condition_with_spaces(self):
        """Тест парсинга условия с пробелами."""
        condition = parse_condition('price > 500')
        assert condition.column == 'price'
        assert condition.operator == '>'
        assert condition.value == '500'
    
    def test_parse_condition_complex_operator(self):
        """Тест парсинга условия с составным оператором."""
        condition = parse_condition('price>=500')
        assert condition.column == 'price'
        assert condition.operator == '>='
        assert condition.value == '500'
    
    def test_parse_empty_condition(self):
        """Тест парсинга пустого условия."""
        with pytest.raises(ValidationError):
            parse_condition('')
    
    def test_parse_condition_empty_column(self):
        """Тест парсинга условия с пустой колонкой."""
        with pytest.raises(ValidationError):
            parse_condition('>500')
    
    def test_parse_condition_empty_value(self):
        """Тест парсинга условия с пустым значением."""
        with pytest.raises(ValidationError):
            parse_condition('price>')
    
    def test_parse_invalid_condition(self):
        """Тест парсинга неверного условия."""
        with pytest.raises(ValidationError):
            parse_condition('invalid_condition')
    
    def test_parse_aggregate(self):
        """Тест парсинга агрегации."""
        column, aggregator_type = parse_aggregate('price=avg')
        assert column == 'price'
        assert aggregator_type == 'avg'
    
    def test_parse_aggregate_with_spaces(self):
        """Тест парсинга агрегации с пробелами."""
        column, aggregator_type = parse_aggregate('price = avg')
        assert column == 'price'
        assert aggregator_type == 'avg'
    
    def test_parse_empty_aggregate(self):
        """Тест парсинга пустой агрегации."""
        with pytest.raises(ValidationError):
            parse_aggregate('')
    
    def test_parse_aggregate_empty_column(self):
        """Тест парсинга агрегации с пустой колонкой."""
        with pytest.raises(ValidationError):
            parse_aggregate('=avg')
    
    def test_parse_aggregate_empty_type(self):
        """Тест парсинга агрегации с пустым типом."""
        with pytest.raises(ValidationError):
            parse_aggregate('price=')
    
    def test_parse_invalid_aggregate(self):
        """Тест парсинга неверной агрегации."""
        with pytest.raises(ValidationError):
            parse_aggregate('invalid_aggregate')
    
    def test_parse_order_by(self):
        """Тест парсинга сортировки."""
        sorter = parse_order_by('price=desc')
        assert sorter.column == 'price'
        assert sorter.direction == 'desc'
    
    def test_parse_order_by_with_spaces(self):
        """Тест парсинга сортировки с пробелами."""
        sorter = parse_order_by('price = asc')
        assert sorter.column == 'price'
        assert sorter.direction == 'asc'
    
    def test_parse_empty_order_by(self):
        """Тест парсинга пустой сортировки."""
        with pytest.raises(ValidationError):
            parse_order_by('')
    
    def test_parse_order_by_empty_column(self):
        """Тест парсинга сортировки с пустой колонкой."""
        with pytest.raises(ValidationError):
            parse_order_by('=desc')
    
    def test_parse_order_by_empty_direction(self):
        """Тест парсинга сортировки с пустым направлением."""
        with pytest.raises(ValidationError):
            parse_order_by('price=')
    
    def test_parse_invalid_order_by(self):
        """Тест парсинга неверной сортировки."""
        with pytest.raises(ValidationError):
            parse_order_by('invalid_order_by')


class TestIntegration:
    """Интеграционные тесты."""
    
    @pytest.fixture
    def sample_csv_file(self):
        """Создает временный CSV-файл для интеграционных тестов."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("name,brand,price,rating\n")
            f.write("iphone 15 pro,apple,999,4.9\n")
            f.write("galaxy s23 ultra,samsung,1199,4.8\n")
            f.write("redmi note 12,xiaomi,199,4.6\n")
            f.write("poco x5 pro,xiaomi,299,4.4\n")
            f.write("pixel 8 pro,google,899,4.7\n")
            f.write("oneplus 11,oneplus,699,4.5\n")
            f.flush()  # Убеждаемся, что данные записаны
            yield f.name
        # Удаляем файл после теста
        if os.path.exists(f.name):
            os.unlink(f.name)
    
    def test_full_filtering_workflow(self, sample_csv_file):
        """Тест полного процесса фильтрации."""
        processor = CSVProcessor(sample_csv_file)
        processor.load_data()
        
        condition = parse_condition('brand=apple')
        filtered_data = processor.filter_data(condition)
        
        assert len(filtered_data) == 1
        assert filtered_data[0]['name'] == 'iphone 15 pro'
    
    def test_full_aggregation_workflow(self, sample_csv_file):
        """Тест полного процесса агрегации."""
        processor = CSVProcessor(sample_csv_file)
        processor.load_data()
        
        column, aggregator_type = parse_aggregate('price=max')
        aggregator = AggregatorFactory.create(aggregator_type)
        result = processor.aggregate_data(column, aggregator)
        
        assert result['result'] == 1199.0
    
    def test_full_sorting_workflow(self, sample_csv_file):
        """Тест полного процесса сортировки."""
        processor = CSVProcessor(sample_csv_file)
        processor.load_data()
        
        sorter = parse_order_by('price=desc')
        sorted_data = processor.sort_data(sorter)
        
        prices = [int(row['price']) for row in sorted_data]
        assert prices == [1199, 999, 899, 699, 299, 199]
    
    def test_new_aggregators_workflow(self, sample_csv_file):
        """Тест новых агрегаторов."""
        processor = CSVProcessor(sample_csv_file)
        processor.load_data()
        
        # Тест медианы
        column, aggregator_type = parse_aggregate('price=median')
        aggregator = AggregatorFactory.create(aggregator_type)
        result = processor.aggregate_data(column, aggregator)
        assert result['result'] == 799.0  # (699 + 899) / 2
        
        # Тест суммы
        column, aggregator_type = parse_aggregate('price=sum')
        aggregator = AggregatorFactory.create(aggregator_type)
        result = processor.aggregate_data(column, aggregator)
        assert result['result'] == 4294.0  # 999 + 1199 + 199 + 299 + 899 + 699
        
        # Тест подсчета
        column, aggregator_type = parse_aggregate('price=count')
        aggregator = AggregatorFactory.create(aggregator_type)
        result = processor.aggregate_data(column, aggregator)
        assert result['result'] == 6 