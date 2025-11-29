from src.data_processor import DataProcessor
from src.recommendation_engine import AdvancedRecommendationEngine
from src.category_discovery import CategoryDiscoverer
from utils.metrics import RecommendationMetrics
import json
from datetime import datetime
import os
import polars as pl

def main():
    print("🚀 ЗАПУСК УЛУЧШЕННОЙ СИСТЕМЫ С ML/LLM")
    print("=" * 50)
    
    # 1. Загрузка данных
    print("📊 1. Загрузка данных...")
    processor = DataProcessor()
    data = processor.load_all_data(sample_fraction=0.1)
    
    if not data or 'retail_items' not in data or data['retail_items'].height == 0:
        print("❌ Нет данных для обработки")
        return
    
    # 2. Исправляем логарифмические цены
    print("🔧 2. Исправление логарифмических цен...")
    data['retail_items'] = processor.fix_log_prices(data['retail_items'])
    
    # 3. Анализ структуры данных
    print("🔍 3. Анализ структуры данных...")
    processor.explore_data_structure(data)
    
    # 4. Обнаружение категорий из реальных данных
    print("🎯 4. Обнаружение категорий из данных...")
    discoverer = CategoryDiscoverer()
    categories = discoverer.discover_categories_from_data(data['retail_items'])
    
    print(f"   📁 Найдено категорий: {len(categories.get('existing_categories', {}).get('stats', []))}")
    
    # 5. Инициализация движка с реальными категориями
    print("🤖 5. Инициализация ML/LLM движка...")
    engine = AdvancedRecommendationEngine(discovered_categories=categories)
    
    # 6. Генерация рекомендаций с ML
    print("🎯 6. Генерация рекомендаций...")
    recommendations = engine.generate_recommendations(
        users_df=data['users'],
        events_df=data['retail_events'], 
        items_df=data['retail_items'],
        optimization_strategy="balanced"
    )
    
    if recommendations.height == 0:
        print("❌ Не удалось сгенерировать рекомендации")
        return
    
    # 7. Создание профилей для метрик
    print("👤 7. Создание пользовательских профилей...")
    user_profiles = engine.user_profiler.create_user_profiles(
        data['users'], data['retail_events'], data['retail_items']
    )
    
    # 8. Расчет метрик
    print("📈 8. Расчет метрик качества...")
    metrics_calculator = RecommendationMetrics()
    metrics = metrics_calculator.calculate_all_metrics(recommendations, user_profiles)
    
    # 9. Показать результаты
    print("\n" + "=" * 60)
    print("🎉 РЕЗУЛЬТАТЫ СИСТЕМЫ С ML/LLM")
    print("=" * 60)
    
    # Общая статистика
    print(f"📊 Общая статистика:")
    print(f"   • Рекомендаций сгенерировано: {recommendations.height}")
    print(f"   • Уникальных пользователей: {recommendations['user_id'].n_unique()}")
    print(f"   • Уникальных продуктов: {recommendations['product_id'].n_unique()}")
    
    # ML/LLM статистика
    if 'ml_enhanced' in recommendations.columns:
        ml_enhanced = recommendations.filter(pl.col('ml_enhanced') == True).height
        print(f"   • ML-оптимизировано: {ml_enhanced} ({ml_enhanced/recommendations.height*100:.1f}%)")
    
    if 'llm_enhanced' in recommendations.columns:
        llm_enhanced = recommendations.filter(pl.col('llm_enhanced') == True).height
        print(f"   • LLM-обогащено: {llm_enhanced} ({llm_enhanced/recommendations.height*100:.1f}%)")
    
    # Метрики качества
    print(f"🎯 Качество рекомендаций:")
    print(f"   • Общий score: {metrics['overall_score']['overall_score']}")
    print(f"   • Релевантность: {metrics['relevance']['avg_match_score']}")
    print(f"   • Покрытие: {metrics['coverage']['user_coverage_rate'] * 100:.1f}%")
    print(f"   • Диверсификация: {metrics['diversity']['diversity_index']}")
    
    # Примеры рекомендаций - ИСПРАВЛЕННЫЙ КОД
    print(f"\n📋 Примеры рекомендаций:")
    sample_recs = recommendations.head(3)
    for i, rec in enumerate(sample_recs.iter_rows(named=True), 1):
        # ИСПРАВЛЕНИЕ: безопасное получение и преобразование user_id
        user_id = rec.get('user_id', 'unknown')
        if isinstance(user_id, (int, float)):
            user_id_str = str(int(user_id))
        else:
            user_id_str = str(user_id)
        
        product_name = rec.get('product_name', 'unknown')
        final_score = rec.get('final_score', 0)
        
        print(f"   {i}. 👤 {user_id_str[:8]}... → 📦 {product_name}")
        print(f"      ⭐ Score: {final_score}")
        
        if 'llm_explanation' in rec and rec['llm_explanation']:
            explanation = rec['llm_explanation']
            if len(explanation) > 80:
                explanation = explanation[:77] + "..."
            print(f"      💡 {explanation}")
            
        if i < 3:
            print()
    
    # 10. Сохранение результатов
    print(f"\n💾 9. Сохранение результатов...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Создаем папку results если нет
    os.makedirs("results", exist_ok=True)
    
    # Сохраняем рекомендации
    recommendations.write_parquet(f"results/recommendations_{timestamp}.parquet")
    
    # Сохраняем метрики
    with open(f"results/metrics_{timestamp}.json", 'w', encoding='utf-8') as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    
    # Генерируем отчет
    report = metrics_calculator.generate_metrics_report(metrics)
    with open(f"results/report_{timestamp}.txt", 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"✅ Результаты сохранены в папке 'results/'")
    print(f"   • recommendations_{timestamp}.parquet")
    print(f"   • metrics_{timestamp}.json") 
    print(f"   • report_{timestamp}.txt")

if __name__ == "__main__":
    main()