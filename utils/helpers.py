import polars as pl
import numpy as np
from pathlib import Path
from typing import Any, Dict, List, Union
import json
import time
from datetime import datetime
import logging

class SystemHelpers:
    """Вспомогательные функции для системы"""
    
    @staticmethod
    def setup_logging(log_file: str = "psb_recommendation.log"):
        """Настройка логирования"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    
    @staticmethod
    def timer(func):
        """Декоратор для измерения времени выполнения"""
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            print(f"⏱️  {func.__name__} выполнено за {end_time - start_time:.2f} секунд")
            return result
        return wrapper
    
    @staticmethod
    def safe_read_parquet(filepath: Path, **kwargs) -> pl.DataFrame:
        """Безопасное чтение parquet файлов"""
        try:
            if filepath.exists():
                return pl.read_parquet(filepath, **kwargs)
            else:
                print(f"⚠️ Файл не найден: {filepath}")
                return pl.DataFrame()
        except Exception as e:
            print(f"❌ Ошибка чтения {filepath}: {e}")
            return pl.DataFrame()
    
    @staticmethod
    def save_dataframe(df: pl.DataFrame, filepath: Path, verbose: bool = True):
        """Сохранение DataFrame с обработкой ошибок"""
        try:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            df.write_parquet(filepath)
            if verbose:
                print(f"💾 Сохранено: {filepath} ({df.height} строк)")
        except Exception as e:
            print(f"❌ Ошибка сохранения {filepath}: {e}")
    
    @staticmethod
    def sample_dataframe(df: pl.DataFrame, sample_size: Union[int, float], 
                        random_state: int = 42) -> pl.DataFrame:
        """Семплирование DataFrame"""
        if df.height == 0:
            return df
        
        if isinstance(sample_size, float):
            sample_size = int(df.height * sample_size)
        
        sample_size = min(sample_size, df.height)
        return df.sample(n=sample_size, seed=random_state)
    
    @staticmethod
    def print_dataframe_info(df: pl.DataFrame, name: str = "DataFrame"):
        """Печать информации о DataFrame"""
        if df.height == 0:
            print(f"📊 {name}: Пустой DataFrame")
            return
        
        print(f"📊 {name}:")
        print(f"   Форма: {df.height} строк, {df.width} колонок")
        print(f"   Колонки: {df.columns}")
        print(f"   Типы: {df.dtypes}")
        
        # Статистика по числовым колонкам
        numeric_cols = [col for col in df.columns if df[col].dtype in [pl.Int64, pl.Float64]]
        if numeric_cols:
            print(f"   Числовые колонки: {numeric_cols}")
    
    @staticmethod
    def merge_multiple_dataframes(df_list: List[pl.DataFrame], on: str = 'user_id') -> pl.DataFrame:
        """Объединение нескольких DataFrame"""
        if not df_list:
            return pl.DataFrame()
        
        result = df_list[0]
        for df in df_list[1:]:
            if df.height > 0:
                result = result.join(df, on=on, how='left')
        
        return result
    
    @staticmethod
    def detect_outliers_iqr(df: pl.DataFrame, column: str) -> Dict:
        """Обнаружение выбросов методом IQR"""
        if column not in df.columns:
            return {}
        
        values = df[column].drop_nulls()
        if values.len() == 0:
            return {}
        
        q1 = values.quantile(0.25)
        q3 = values.quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        outliers = df.filter(
            (pl.col(column) < lower_bound) | (pl.col(column) > upper_bound)
        )
        
        return {
            'outlier_count': outliers.height,
            'outlier_percentage': outliers.height / df.height,
            'bounds': {'lower': lower_bound, 'upper': upper_bound},
            'outliers_sample': outliers.select([column]).head(5).to_dicts()
        }

class DataValidator:
    """Класс для валидации данных"""
    
    @staticmethod
    def validate_user_profiles(profiles: pl.DataFrame) -> Dict:
        """Валидация пользовательских профилей"""
        validation_results = {
            'total_profiles': profiles.height,
            'missing_values': {},
            'data_quality_issues': [],
            'validation_passed': True
        }
        
        if profiles.height == 0:
            validation_results['validation_passed'] = False
            validation_results['data_quality_issues'].append("Нет данных профилей")
            return validation_results
        
        # Проверка обязательных полей
        required_fields = ['user_id', 'total_spent', 'spending_level']
        for field in required_fields:
            if field not in profiles.columns:
                validation_results['data_quality_issues'].append(f"Отсутствует обязательное поле: {field}")
                validation_results['validation_passed'] = False
        
        # Проверка пропущенных значений
        for column in profiles.columns:
            null_count = profiles[column].null_count()
            if null_count > 0:
                validation_results['missing_values'][column] = {
                    'count': null_count,
                    'percentage': null_count / profiles.height
                }
        
        # Проверка аномальных значений
        if 'total_spent' in profiles.columns:
            negative_spending = profiles.filter(pl.col('total_spent') < 0).height
            if negative_spending > 0:
                validation_results['data_quality_issues'].append(f"Обнаружены отрицательные траты: {negative_spending} записей")
        
        return validation_results
    
    @staticmethod
    def validate_recommendations(recommendations: pl.DataFrame) -> Dict:
        """Валидация рекомендаций"""
        validation_results = {
            'total_recommendations': recommendations.height,
            'score_validation': {},
            'business_rules_violations': [],
            'validation_passed': True
        }
        
        if recommendations.height == 0:
            validation_results['validation_passed'] = False
            validation_results['business_rules_violations'].append("Нет рекомендаций")
            return validation_results
        
        # Проверка scores
        if 'match_score' in recommendations.columns:
            invalid_scores = recommendations.filter(
                (pl.col('match_score') < 0) | (pl.col('match_score') > 1)
            ).height
            if invalid_scores > 0:
                validation_results['score_validation']['invalid_match_scores'] = invalid_scores
                validation_results['validation_passed'] = False
        
        # Проверка дубликатов рекомендаций
        duplicates = recommendations.filter(
            pl.col('user_id').is_duplicated() & pl.col('product_id').is_duplicated()
        ).height
        if duplicates > 0:
            validation_results['business_rules_violations'].append(f"Обнаружены дубликаты рекомендаций: {duplicates}")
        
        return validation_results

class FormatHelpers:
    """Класс для форматирования вывода"""
    
    @staticmethod
    def format_currency(amount: float) -> str:
        """Форматирование валюты"""
        if amount >= 1_000_000:
            return f"{amount/1_000_000:.1f}M₽"
        elif amount >= 1_000:
            return f"{amount/1_000:.1f}K₽"
        else:
            return f"{amount:.0f}₽"
    
    @staticmethod
    def format_percentage(value: float) -> str:
        """Форматирование процентов"""
        return f"{value*100:.1f}%"
    
    @staticmethod
    def format_large_number(number: int) -> str:
        """Форматирование больших чисел"""
        if number >= 1_000_000:
            return f"{number/1_000_000:.1f}M"
        elif number >= 1_000:
            return f"{number/1_000:.1f}K"
        else:
            return f"{number}"
    
    @staticmethod
    def create_progress_bar(iteration: int, total: int, length: int = 50) -> str:
        """Создание прогресс-бара"""
        percent = ("{0:.1f}").format(100 * (iteration / float(total)))
        filled_length = int(length * iteration // total)
        bar = '█' * filled_length + '─' * (length - filled_length)
        return f"|{bar}| {percent}%"