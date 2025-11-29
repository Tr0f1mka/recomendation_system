import polars as pl
import glob
from pathlib import Path
from config.paths import PATHS
from typing import Dict, List
import json

class DataProcessor:
    def __init__(self):
        self.paths = PATHS
        self.processed_data = {}
    
    def load_all_data(self, sample_fraction: float = 0.1) -> Dict:
        """Загрузка всех данных с возможностью семплирования"""
        print("🔄 Загрузка данных...")
        
        data = {}
        
        try:
            # Пользователи
            data['users'] = pl.read_parquet(self.paths.RAW_DATA['users'])
            print(f"👥 Пользователи: {data['users'].shape}")
            
            # Retail items
            data['retail_items'] = pl.read_parquet(self.paths.RAW_DATA['retail_items'])
            print(f"🛍️ Retail товары: {data['retail_items'].shape}")
            print(f"   Колонки: {data['retail_items'].columns}")
            
            # Семплирование событий для скорости
            data['retail_events'] = self._load_and_sample_events('retail', sample_fraction)
            print(f"📊 Retail события: {data['retail_events'].shape}")
            
        except Exception as e:
            print(f"❌ Ошибка загрузки: {e}")
            
        return data
    
    def _load_and_sample_events(self, event_type: str, fraction: float) -> pl.DataFrame:
        """Загрузка и семплирование событий"""
        event_files = glob.glob(str(self.paths.EVENT_PATTERNS[event_type]))
        print(f"   Найдено файлов {event_type}: {len(event_files)}")
        
        if not event_files:
            return pl.DataFrame()
        
        # Берем первый файл для демо
        sample_file = event_files[0]
        events = pl.read_parquet(sample_file)
        
        # Семплируем если данных много
        if events.height > 100000:
            events = events.sample(fraction=fraction)
            
        return events
    
    def explore_data_structure(self, data: Dict):
        """Исследование структуры данных"""
        print("\n🔍 Анализ структуры данных:")
        
        # Анализ retail items
        if 'retail_items' in data:
            items_df = data['retail_items']
            print(f"\n📦 Retail Items анализ:")
            print(f"   Колонки: {items_df.columns}")
            print(f"   Типы данных: {items_df.dtypes}")
            
            try:
                # Уникальные значения в категориях
                if 'category' in items_df.columns:
                    categories = items_df['category'].unique().to_list()
                    valid_categories = [c for c in categories if c is not None]
                    print(f"   Уникальные категории: {len(valid_categories)}")
                    print(f"   Примеры: {valid_categories[:5]}")
                    print(f"   Товаров без категории: {items_df.filter(pl.col('category').is_null()).height}")
                    
                if 'subcategory' in items_df.columns:
                    subcategories = items_df['subcategory'].unique().to_list()
                    valid_subcategories = [s for s in subcategories if s is not None]
                    print(f"   Уникальные подкатегории: {len(valid_subcategories)}")
                    print(f"   Примеры: {valid_subcategories[:5]}")
                    
                if 'price' in items_df.columns:
                    # Детальный анализ цен
                    price_stats = items_df.select([
                        pl.col('price').min().alias('min_price'),
                        pl.col('price').max().alias('max_price'),
                        pl.col('price').mean().alias('mean_price'),
                        pl.col('price').std().alias('std_price'),
                        pl.col('price').quantile(0.25).alias('q25_price'),
                        pl.col('price').quantile(0.75).alias('q75_price'),
                        pl.col('price').median().alias('median_price')
                    ]).row(0)
                    
                    print(f"   Статистика цен:")
                    print(f"     Мин: {price_stats[0]:.2f}")
                    print(f"     Макс: {price_stats[1]:.2f}")
                    print(f"     Среднее: {price_stats[2]:.2f}")
                    print(f"     Медиана: {price_stats[6]:.2f}")
                    print(f"     25% перцентиль: {price_stats[4]:.2f}")
                    print(f"     75% перцентиль: {price_stats[5]:.2f}")
                    
                    # Анализ проблем с ценами
                    negative_prices = items_df.filter(pl.col('price') < 0).height
                    zero_prices = items_df.filter(pl.col('price') == 0).height
                    suspicious_prices = items_df.filter(pl.col('price').abs() < 1).height
                    
                    print(f"   Проблемы с ценами:")
                    print(f"     Отрицательные цены: {negative_prices}")
                    print(f"     Нулевые цены: {zero_prices}")
                    print(f"     Подозрительно малые цены (<1): {suspicious_prices}")
                    
                    # Предположение о масштабе цен
                    if price_stats[1] < 10 and price_stats[2] < 0:
                        print("   ⚠️ Возможно, цены в логарифмической шкале или нормализованы")
                        
            except Exception as e:
                print(f"   ⚠️ Ошибка анализа: {e}")
        
    def save_processed_data(self, data: Dict, filename: str):
        """Сохранение обработанных данных"""
        self.paths.ensure_directories()
        filepath = self.paths.PROCESSED_DIR / f"{filename}.parquet"
        
        # Сохраняем первый датафрейм как пример
        for key, df in data.items():
            if isinstance(df, pl.DataFrame) and df.height > 0:
                df.write_parquet(filepath)
                print(f"💾 Сохранено: {filepath}")
                break
    def fix_log_prices(self, items_df: pl.DataFrame) -> pl.DataFrame:
        """Исправление логарифмических цен"""
        print("🔧 Исправление логарифмических цен...")
        
        if 'price' not in items_df.columns:
            return items_df
        
        # Преобразуем логарифмические цены обратно в нормальные
        items_df = items_df.with_columns([
            pl.when(pl.col('price') <= 0)
            .then(pl.lit(1000))  # Минимальная цена для отрицательных значений
            .otherwise(pl.col('price').exp())  # exp() для обратного преобразования логарифма
            .alias('price_fixed')
        ])
        
        # Статистика после исправления
        price_stats = items_df.select([
            pl.col('price_fixed').min().alias('min_price'),
            pl.col('price_fixed').max().alias('max_price'),
            pl.col('price_fixed').mean().alias('mean_price'),
        ]).row(0)
        
        print(f"   ✅ Цены исправлены:")
        print(f"      Мин: {price_stats[0]:.2f}₽")
        print(f"      Макс: {price_stats[1]:.2f}₽") 
        print(f"      Среднее: {price_stats[2]:.2f}₽")
        
        return items_df