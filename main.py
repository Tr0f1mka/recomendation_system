import sys
from pathlib import Path
import polars as pl  # ДОБАВЬТЕ ЭТУ СТРОКУ

# Добавляем корневую директорию в путь
sys.path.append(str(Path(__file__).parent))

from src.data_processor import DataProcessor
from src.category_discovery import CategoryDiscoverer
from src.user_profiler import UserProfiler
from src.product_matcher import ProductMatcher
from src.recommendation_engine import AdvancedRecommendationEngine
from utils.metrics import RecommendationMetrics
from config.paths import PATHS
import json

class PSBRecommendationSystem:
    def __init__(self):
        self.data_processor = DataProcessor()
        self.category_discoverer = CategoryDiscoverer()
        self.user_profiler = None
        self.product_matcher = None
        self.recommendation_engine = None
        self.metrics_calculator = RecommendationMetrics()
        
    def run_full_pipeline(self, sample_fraction: float = 0.01, 
                         run_complete_analysis: bool = True):
        """Запуск полного пайплайна рекомендательной системы"""
        print("🚀 Запуск PSB Recommendation System")
        print("=" * 50)
        
        # Этап 1: Загрузка и анализ данных
        print("\n1. 📊 ЗАГРУЗКА ДАННЫХ")
        data = self.data_processor.load_all_data(sample_fraction)
        self.data_processor.explore_data_structure(data)
        
        if not data or 'retail_items' not in data:
            print("❌ Недостаточно данных для продолжения")
            return
        
        if 'retail_items' in data:
            print("\n🔧 Преобразование цен в нормальный масштаб...")
            items_df = data['retail_items']
    
        # Создаем реалистичные цены на основе категорий
        data['retail_items'] = items_df.with_columns([
            pl.col('price').alias('price_original'),
            # Создаем реалистичные цены на основе хэша категории
            pl.when(pl.col('category').is_not_null())
            .then((pl.col('category').hash() % 9000) + 1000)  # Цены от 1000 до 10000
            .otherwise(2000)  # Базовая цена для товаров без категории
            .alias('price_fixed')
            ])
    
        new_prices = data['retail_items']['price_fixed']
        print(f"   Исправленные цены: мин={new_prices.min():.2f}, макс={new_prices.max():.2f}, среднее={new_prices.mean():.2f}")
            # Продолжаем обычный пайплайн
        print("\n2. 🎯 ОБНАРУЖЕНИЕ КАТЕГОРИЙ")
        discovered_categories = self.category_discoverer.discover_categories_from_data(data['retail_items'])
        
        print("📋 Обнаруженные категории:")
        if 'existing_categories' in discovered_categories:
            top_cats = discovered_categories['existing_categories'].get('top_categories', [])
            for cat in top_cats[:5]:
                print(f"   {cat['category']}: {cat['item_count']} товаров")
        
        # Инициализация движков
        self.recommendation_engine = AdvancedRecommendationEngine(discovered_categories)
        
        if run_complete_analysis:
            # Запуск полного анализа
            results = self.recommendation_engine.run_complete_analysis(
                data['users'],
                data['retail_events'], 
                data['retail_items'],
                output_dir="results"
            )
            
            # Вывод ключевых результатов
            self._print_key_results(results)
        else:
            # Базовый пайплайн
            self._run_basic_pipeline(data, discovered_categories)
        
        print("✅ Пайплайн завершен!")
    
    def _run_basic_pipeline(self, data: dict, discovered_categories: dict):
        """Запуск базового пайплайна"""
        # Создание профилей пользователей
        self.user_profiler = UserProfiler(discovered_categories)
        user_profiles = self.user_profiler.create_user_profiles(
            data['users'], data['retail_events'], data['retail_items']
        )
        
        # Генерация рекомендаций
        self.product_matcher = ProductMatcher(discovered_categories)
        recommendations = self.product_matcher.match_users_to_products(user_profiles)
        
        # Расчет метрик
        metrics = self.metrics_calculator.calculate_all_metrics(recommendations, user_profiles)
        
        # Вывод результатов
        print("\n📊 РЕЗУЛЬТАТЫ БАЗОВОГО ПАЙПЛАЙНА:")
        print(f"   👤 Профилей создано: {user_profiles.height}")
        print(f"   🎯 Рекомендаций сгенерировано: {recommendations.height}")
        print(f"   📈 Общий score системы: {metrics['overall_score']['overall_score']}")
        
        # Сохранение результатов
        self._save_basic_results(user_profiles, recommendations, metrics, discovered_categories)
    
    def _print_key_results(self, results: dict):
        """Вывод ключевых результатов полного анализа"""
        print("\n" + "=" * 60)
        print("🏆 КЛЮЧЕВЫЕ РЕЗУЛЬТАТЫ")
        print("=" * 60)
        
        if 'strategy_comparison' in results:
            best_strategy = results['strategy_comparison']['best_strategy']
            print(f"🎯 Лучшая стратегия: {best_strategy['strategy']}")
            print(f"   Weighted Score: {best_strategy['weighted_score']:.3f}")
            print(f"   Причина: {best_strategy['reason']}")
        
        if 'metrics' in results:
            metrics = results['metrics']
            overall = metrics['overall_score']
            print(f"\n📊 Общая оценка системы: {overall['overall_score']} ({overall['quality_rating']})")
            
            business = metrics['business']
            print(f"💰 Бизнес-воздействие: {business['avg_business_value_per_rec']:.3f}")
            if 'estimated_revenue_impact' in business:
                print(f"   Оценка выручки: {business['estimated_revenue_impact']['estimated_total_impact']:,.0f}₽")
        
        if 'recommendations' in results:
            recs = results['recommendations']
            print(f"\n📈 Статистика рекомендаций:")
            print(f"   Всего рекомендаций: {recs.height}")
            print(f"   Уникальных пользователей: {recs['user_id'].unique().length()}")
            if 'final_score' in recs.columns:
                print(f"   Средний score: {recs['final_score'].mean():.3f}")
    
    def _save_basic_results(self, user_profiles, recommendations, metrics, discovered_categories):
        """Сохранение результатов базового пайплайна"""
        PATHS.ensure_directories()
        
        # Сохраняем данные
        user_profiles.write_parquet(PATHS.PROCESSED_DIR / "user_profiles.parquet")
        recommendations.write_parquet(PATHS.PROCESSED_DIR / "product_recommendations.parquet")
        
        # Сохраняем метрики и категории
        with open(PATHS.PROCESSED_DIR / "system_metrics.json", 'w', encoding='utf-8') as f:
            json.dump(metrics, f, ensure_ascii=False, indent=2)
        
        with open(PATHS.PROCESSED_DIR / "discovered_categories.json", 'w', encoding='utf-8') as f:
            json.dump(discovered_categories, f, ensure_ascii=False, indent=2)
        
        print(f"💾 Результаты сохранены в {PATHS.PROCESSED_DIR}")

if __name__ == "__main__":
    system = PSBRecommendationSystem()
    
    # Запуск с разными опциями:
    
    # 1. Быстрый тест (1% данных, базовый пайплайн)
    # system.run_full_pipeline(sample_fraction=0.01, run_complete_analysis=False)
    
    # 2. Полный анализ (1% данных)
    system.run_full_pipeline(sample_fraction=0.01, run_complete_analysis=True)
    
    # 3. Продакшн-режим (больше данных)
    # system.run_full_pipeline(sample_fraction=0.1, run_complete_analysis=True)