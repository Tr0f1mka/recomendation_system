from src.data_processor import DataProcessor
from src.recommendation_engine import AdvancedRecommendationEngine
from src.category_discovery import CategoryDiscoverer
import polars as pl  # ДОБАВИЛ ИМПОРТ

def main():
    print("🚀 ТЕСТ С ML ОПТИМИЗАЦИЕЙ")
    
    # Загружаем данные
    processor = DataProcessor()
    data = processor.load_all_data(sample_fraction=0.05)
    data['retail_items'] = processor.fix_log_prices(data['retail_items'])
    
    # Обнаружение категорий
    categories = CategoryDiscoverer().discover_categories_from_data(data['retail_items'])
    
    # Создаем движок
    engine = AdvancedRecommendationEngine(discovered_categories=categories)
    
    # Генерируем рекомендации с ML
    print("🎯 Генерация рекомендаций с ML...")
    recommendations = engine.generate_recommendations(
        data['users'], data['retail_events'], data['retail_items'],
        optimization_strategy="balanced"
    )
    
    if recommendations.height > 0:
        ml_count = recommendations.filter(pl.col('ml_enhanced') == True).height
        total_count = recommendations.height
        print(f"📊 ML статистика: {ml_count}/{total_count} ({ml_count/total_count*100:.1f}%)")
        
        # Покажем несколько примеров
        sample = recommendations.filter(pl.col('ml_enhanced') == True).head(2)
        for rec in sample.iter_rows(named=True):
            print(f"👤 User: {rec['user_id']}")
            print(f"📦 Product: {rec['product_name']}")
            print(f"⭐ Base: {rec['base_match_score']} -> Final: {rec['final_score']}")
            print(f"🤖 ML used: {rec['ml_enhanced']}")
            print("---")

if __name__ == "__main__":
    main()