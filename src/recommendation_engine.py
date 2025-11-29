import polars as pl
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
import json
from pathlib import Path  # ДОБАВЬТЕ ЭТУ СТРОКУ

from src.user_profiler import UserProfiler
from src.product_matcher import ProductMatcher
from utils.metrics import RecommendationMetrics
from utils.helpers import SystemHelpers, DataValidator

class AdvancedRecommendationEngine:
    """Продвинутый движок рекомендаций с оптимизацией"""
    
    def __init__(self, discovered_categories: Dict):
        self.discovered_categories = discovered_categories
        self.user_profiler = UserProfiler(discovered_categories)
        self.product_matcher = ProductMatcher(discovered_categories)
        self.metrics_calculator = RecommendationMetrics()
        self.helpers = SystemHelpers()
        
        # Кэширование для производительности
        self._user_profiles_cache = {}
        self._recommendations_cache = {}
    
    # ... остальной код остается без изменений ...
        
    def generate_recommendations(self, users_df: pl.DataFrame, 
                           events_df: pl.DataFrame, 
                           items_df: pl.DataFrame,
                           optimization_strategy: str = "balanced") -> pl.DataFrame:  # УБИРАЕМ use_ml
        """
        Генерация оптимизированных рекомендаций с ML/LLM
        """
        print(f"🎯 Генерация рекомендаций (стратегия: {optimization_strategy})...")
        
        # Создание профилей пользователей
        user_profiles = self.user_profiler.create_user_profiles(users_df, events_df, items_df)
        
        if user_profiles.height == 0:
            print("❌ Не удалось создать профили пользователей")
            return pl.DataFrame()
        
        # Генерация рекомендаций с ML/LLM (ВСЕГДА используем ML)
        recommendations = self.product_matcher.match_users_to_products(user_profiles, use_ml=True)
        
        if recommendations.height == 0:
            print("❌ Не удалось сгенерировать рекомендации")
            return pl.DataFrame()
        
        # Применение стратегии оптимизации
        optimized_recommendations = self._apply_optimization_strategy(
            recommendations, optimization_strategy
        )
        
        # Валидация результатов
        validation_results = DataValidator.validate_recommendations(optimized_recommendations)
        
        if not validation_results['validation_passed']:
            print("⚠️ Предупреждения валидации:")
            for issue in validation_results.get('business_rules_violations', []):
                print(f"   - {issue}")
        
        print(f"✅ Сгенерировано рекомендаций: {optimized_recommendations.height}")
        
        return optimized_recommendations
    
    def _apply_optimization_strategy(self, recommendations: pl.DataFrame, 
                               strategy: str) -> pl.DataFrame:
        """Применение стратегии оптимизации к рекомендациям"""
        
        if strategy == "coverage":
            return self._optimize_for_coverage(recommendations)
        elif strategy == "revenue":
            return self._optimize_for_revenue(recommendations)
        elif strategy == "engagement":
            return self._optimize_for_engagement(recommendations)
        else:  # balanced
            return self._optimize_balanced(recommendations)

    def _optimize_for_coverage(self, recommendations: pl.DataFrame) -> pl.DataFrame:
        """Оптимизация для максимального покрытия пользователей"""
        print("   🎯 Оптимизация: Максимальное покрытие пользователей")
        
        # Группируем по пользователям и оставляем топ-1 рекомендацию по final_score
        optimized = (
            recommendations.sort(['user_id', 'final_score'], descending=[False, True])
            .group_by('user_id')
            .head(1)
        )
        
        return optimized

    def _optimize_for_revenue(self, recommendations: pl.DataFrame) -> pl.DataFrame:
        """Оптимизация для максимизации бизнес-ценности"""
        print("   💰 Оптимизация: Максимизация бизнес-ценности")
        
        # Взвешенная комбинация final_score и business_value
        optimized = recommendations.with_columns([
            (pl.col('final_score') * 0.4 + pl.col('business_value') * 0.6)
            .alias('revenue_score')
        ]).sort(['user_id', 'revenue_score'], descending=[False, True])
        
        # Оставляем топ рекомендации по revenue_score
        optimized = (
            optimized.group_by('user_id')
            .agg([
                pl.col('product_id').first().alias('product_id'),
                pl.col('product_name').first().alias('product_name'),
                pl.col('product_type').first().alias('product_type'),
                pl.col('base_match_score').first().alias('base_match_score'),
                pl.col('final_score').first().alias('final_score'),
                pl.col('business_value').first().alias('business_value'),
                pl.col('reasoning').first().alias('reasoning'),
                pl.col('llm_explanation').first().alias('llm_explanation'),
                pl.col('ml_enhanced').first().alias('ml_enhanced'),
                pl.col('llm_enhanced').first().alias('llm_enhanced'),
                pl.col('revenue_score').first().alias('final_score')  # Перезаписываем final_score
            ])
        )
        
        return optimized

    def _optimize_for_engagement(self, recommendations: pl.DataFrame) -> pl.DataFrame:
        """Оптимизация для вовлечения пользователей"""
        print("   🔥 Оптимизация: Максимизация вовлечения")
        
        # Предпочтение продуктов с высоким final_score
        optimized = (
            recommendations.sort(['user_id', 'final_score'], descending=[False, True])
            .group_by('user_id')
            .agg([
                pl.col('product_id').first().alias('product_id'),
                pl.col('product_name').first().alias('product_name'),
                pl.col('product_type').first().alias('product_type'),
                pl.col('base_match_score').first().alias('base_match_score'),
                pl.col('final_score').first().alias('final_score'),
                pl.col('business_value').first().alias('business_value'),
                pl.col('reasoning').first().alias('reasoning'),
                pl.col('llm_explanation').first().alias('llm_explanation'),
                pl.col('ml_enhanced').first().alias('ml_enhanced'),
                pl.col('llm_enhanced').first().alias('llm_enhanced')
            ])
        )
        
        return optimized

    def _optimize_balanced(self, recommendations: pl.DataFrame) -> pl.DataFrame:
        """Сбалансированная оптимизация"""
        print("   ⚖️ Оптимизация: Сбалансированный подход")
        
        # Баланс между релевантностью и бизнес-ценностью
        optimized = recommendations.with_columns([
            (pl.col('final_score') * 0.6 + pl.col('business_value') * 0.4)
            .alias('balanced_score')
        ]).sort(['user_id', 'balanced_score'], descending=[False, True])
        
        # Оставляем до 3 рекомендаций на пользователя
        optimized = (
            optimized.group_by('user_id')
            .head(3)
            .with_columns(pl.col('balanced_score').alias('final_score'))  # Обновляем final_score
            .drop('balanced_score')  # Удаляем временную колонку
        )
        
        return optimized
    
    def generate_personalized_explanations(self, recommendations: pl.DataFrame, 
                                         user_profiles: pl.DataFrame) -> pl.DataFrame:
        """Генерация персонализированных объяснений рекомендаций"""
        print("📝 Генерация персонализированных объяснений...")
        
        explanations = []
        
        for rec in recommendations.iter_rows(named=True):
            user_profile = user_profiles.filter(pl.col('user_id') == rec['user_id'])
            
            if user_profile.height > 0:
                profile = user_profile.row(0, named=True)
                explanation = self._create_detailed_explanation(rec, profile)
            else:
                explanation = "На основе общего анализа поведения пользователей"
            
            explanations.append({
                'user_id': rec['user_id'],
                'product_id': rec['product_id'],
                'explanation': explanation,
                'confidence_level': self._get_confidence_level(rec['match_score'])
            })
        
        return pl.DataFrame(explanations)
    
    def _create_detailed_explanation(self, recommendation: Dict, user_profile: Dict) -> str:
        """Создание детального объяснения рекомендации"""
        product_name = recommendation['product_name']
        match_score = recommendation['match_score']
        
        explanation_parts = [f"Рекомендуем {product_name} потому что:"]
        
        # Финансовые аспекты
        if user_profile.get('spending_level') in ['high', 'very_high']:
            explanation_parts.append("• у вас высокий уровень трат, подходящий для этого продукта")
        
        if user_profile.get('avg_transaction_value', 0) > 20000:
            explanation_parts.append("• средний размер ваших покупок соответствует премиальным продуктам")
        
        # Поведенческие аспекты
        if user_profile.get('interaction_frequency') in ['high', 'very_high']:
            explanation_parts.append("• вы проявляете высокую активность, что важно для этого предложения")
        
        if user_profile.get('preference_stability', 0) > 0.7:
            explanation_parts.append("• ваши предпочтения стабильны, что снижает риски")
        
        # Категориальные предпочтения
        category_affinity = user_profile.get('category_affinity', {})
        if category_affinity:
            top_category = next(iter(category_affinity.items()), None)
            if top_category and top_category[1] > 0.5:
                explanation_parts.append(f"• вы часто покупаете в категории '{top_category[0]}'")
        
        # Уровень уверенности
        if match_score > 0.8:
            explanation_parts.append("• это предложение идеально соответствует вашему профилю")
        elif match_score > 0.6:
            explanation_parts.append("• это предложение хорошо соответствует вашему профилю")
        else:
            explanation_parts.append("• это предложение может быть интересно based на вашей активности")
        
        return " ".join(explanation_parts)
    
    def _get_confidence_level(self, match_score: float) -> str:
        """Определение уровня уверенности"""
        if match_score > 0.8:
            return "very_high"
        elif match_score > 0.6:
            return "high"
        elif match_score > 0.4:
            return "medium"
        else:
            return "low"
    
    def analyze_recommendation_impact(self, recommendations: pl.DataFrame,
                                    user_profiles: pl.DataFrame) -> Dict:
        """Анализ потенциального воздействия рекомендаций"""
        print("📊 Анализ воздействия рекомендаций...")
        
        impact_analysis = {}
        
        # Анализ по сегментам пользователей
        user_segments = self._segment_users(user_profiles)
        impact_analysis['user_segments'] = user_segments
        
        # Анализ по типам продуктов
        product_impact = self._analyze_product_impact(recommendations)
        impact_analysis['product_impact'] = product_impact
        
        # Оценка общего воздействия
        total_impact = self._estimate_total_impact(recommendations, user_profiles)
        impact_analysis['total_impact'] = total_impact
        
        return impact_analysis
    
    def _segment_users(self, user_profiles: pl.DataFrame) -> Dict:
        """Сегментация пользователей для анализа"""
        segments = {
            'high_value': user_profiles.filter(pl.col('spending_level').is_in(['high', 'very_high'])),
            'medium_value': user_profiles.filter(pl.col('spending_level') == 'medium'),
            'low_value': user_profiles.filter(pl.col('spending_level').is_in(['low', 'very_low'])),
            'high_activity': user_profiles.filter(pl.col('interaction_frequency').is_in(['high', 'very_high'])),
            'new_users': user_profiles.filter(pl.col('total_interactions') < 10)
        }
        
        segment_stats = {}
        for name, segment_df in segments.items():
            segment_stats[name] = {
                'count': segment_df.height,
                'percentage': segment_df.height / user_profiles.height,
                'avg_spending': segment_df['total_spent'].mean() if segment_df.height > 0 else 0
            }
        
        return segment_stats
    
    def _analyze_product_impact(self, recommendations: pl.DataFrame) -> Dict:
        """Анализ воздействия по типам продуктов"""
        product_analysis = (
            recommendations.group_by('product_type')
            .agg([
                pl.count().alias('recommendation_count'),
                pl.col('match_score').mean().alias('avg_match_score'),
                pl.col('business_value').mean().alias('avg_business_value'),
                pl.col('final_score').mean().alias('avg_final_score'),
                pl.col('user_id').unique().count().alias('unique_users')
            ])
            .sort('recommendation_count', descending=True)
        )
        
        impact_by_product = {}
        for row in product_analysis.iter_rows(named=True):
            product_type = row['product_type']
            impact_by_product[product_type] = {
                'recommendation_count': row['recommendation_count'],
                'unique_users_reached': row['unique_users'],
                'avg_match_score': round(row['avg_match_score'], 3),
                'avg_business_value': round(row['avg_business_value'], 3),
                'avg_final_score': round(row['avg_final_score'], 3),
                'penetration_rate': row['unique_users'] / recommendations['user_id'].unique().length()
            }
        
        return impact_by_product
    
    def _estimate_total_impact(self, recommendations: pl.DataFrame, 
                             user_profiles: pl.DataFrame) -> Dict:
        """Оценка общего воздействия рекомендаций"""
        total_users = user_profiles.height
        users_with_recommendations = recommendations['user_id'].unique().length()
        
        # Оценка потенциальной выручки
        high_value_recommendations = recommendations.filter(pl.col('final_score') > 0.7)
        medium_value_recommendations = recommendations.filter(
            (pl.col('final_score') > 0.5) & (pl.col('final_score') <= 0.7)
        )
        
        # Упрощенная модель конверсии
        high_value_conversion_rate = 0.25  # 25% для высоких scores
        medium_value_conversion_rate = 0.15  # 15% для средних scores
        low_value_conversion_rate = 0.05   # 5% для низких scores
        
        # Средняя ценность продуктов по типам
        product_value_map = {
            'premium_cards': 75000,
            'credit_cards': 25000,
            'savings': 50000,
            'investment': 100000,
            'insurance': 30000
        }
        
        estimated_revenue = 0
        conversion_breakdown = {}
        
        for product_type in recommendations['product_type'].unique().to_list():
            type_recommendations = recommendations.filter(pl.col('product_type') == product_type)
            product_value = product_value_map.get(product_type, 20000)
            
            high_value_count = type_recommendations.filter(pl.col('final_score') > 0.7).height
            medium_value_count = type_recommendations.filter(
                (pl.col('final_score') > 0.5) & (pl.col('final_score') <= 0.7)
            ).height
            low_value_count = type_recommendations.filter(pl.col('final_score') <= 0.5).height
            
            type_revenue = (
                high_value_count * high_value_conversion_rate * product_value +
                medium_value_count * medium_value_conversion_rate * product_value +
                low_value_count * low_value_conversion_rate * product_value
            )
            
            estimated_revenue += type_revenue
            conversion_breakdown[product_type] = {
                'estimated_conversions': round(
                    high_value_count * high_value_conversion_rate +
                    medium_value_count * medium_value_conversion_rate +
                    low_value_count * low_value_conversion_rate
                ),
                'estimated_revenue': round(type_revenue),
                'avg_product_value': product_value
            }
        
        return {
            'estimated_total_revenue': round(estimated_revenue),
            'user_coverage_rate': users_with_recommendations / total_users,
            'avg_recommendations_per_user': recommendations.height / users_with_recommendations,
            'conversion_breakdown': conversion_breakdown,
            'confidence_level': self._calculate_impact_confidence(recommendations)
        }
    
    def _calculate_impact_confidence(self, recommendations: pl.DataFrame) -> str:
        """Расчет уровня уверенности в оценке воздействия"""
        if recommendations.height == 0:
            return "very_low"
        
        avg_final_score = recommendations['final_score'].mean()
        high_confidence_recs = recommendations.filter(pl.col('final_score') > 0.7).height
        high_confidence_ratio = high_confidence_recs / recommendations.height
        
        if high_confidence_ratio > 0.5 and avg_final_score > 0.6:
            return "high"
        elif high_confidence_ratio > 0.3 and avg_final_score > 0.5:
            return "medium"
        elif high_confidence_ratio > 0.1:
            return "low"
        else:
            return "very_low"
    
    def generate_strategy_comparison(self, users_df: pl.DataFrame,
                                   events_df: pl.DataFrame,
                                   items_df: pl.DataFrame) -> Dict:
        """Сравнение разных стратегий оптимизации"""
        print("📊 Сравнение стратегий оптимизации...")
        
        strategies = ['coverage', 'revenue', 'engagement', 'balanced']
        comparison_results = {}
        
        user_profiles = self.user_profiler.create_user_profiles(users_df, events_df, items_df)
        
        for strategy in strategies:
            print(f"   Тестирование стратегии: {strategy}")
            recommendations = self.generate_recommendations(
                users_df, events_df, items_df, strategy
            )
            
            if recommendations.height > 0:
                metrics = self.metrics_calculator.calculate_all_metrics(
                    recommendations, user_profiles
                )
                comparison_results[strategy] = {
                    'metrics': metrics,
                    'recommendation_count': recommendations.height,
                    'unique_users': recommendations['user_id'].unique().length()
                }
        
        # Определение лучшей стратегии
        best_strategy = self._select_best_strategy(comparison_results)
        comparison_results['best_strategy'] = best_strategy
        
        return comparison_results
    
    def _select_best_strategy(self, comparison_results: Dict) -> Dict:
        """Выбор лучшей стратегии на основе метрик"""
        if not comparison_results:
            return {'strategy': 'balanced', 'reason': 'No data available'}
        
        strategy_scores = {}
        
        for strategy, results in comparison_results.items():
            if strategy == 'best_strategy':
                continue
                
            metrics = results['metrics']
            overall_score = metrics['overall_score']['overall_score']
            
            # Взвешенная оценка с учетом бизнес-приоритетов
            business_score = metrics['business']['avg_business_value_per_rec']
            coverage_score = metrics['coverage']['user_coverage_rate']
            relevance_score = metrics['relevance']['avg_match_score']
            
            weighted_score = (
                business_score * 0.4 +
                coverage_score * 0.3 +
                relevance_score * 0.3
            )
            
            strategy_scores[strategy] = {
                'weighted_score': weighted_score,
                'overall_score': overall_score,
                'business_impact': metrics['business']['business_impact'],
                'user_coverage': coverage_score
            }
        
        # Выбор стратегии с максимальным weighted_score
        best_strategy = max(strategy_scores.items(), key=lambda x: x[1]['weighted_score'])
        
        return {
            'strategy': best_strategy[0],
            'weighted_score': best_strategy[1]['weighted_score'],
            'overall_score': best_strategy[1]['overall_score'],
            'reason': f"Оптимальный баланс бизнес-ценности и покрытия пользователей"
        }
    
    def create_recommendation_report(self, recommendations: pl.DataFrame,
                                   user_profiles: pl.DataFrame,
                                   impact_analysis: Dict) -> str:
        """Создание комплексного отчета по рекомендациям"""
        report = []
        report.append("📊 КОМПЛЕКСНЫЙ ОТЧЕТ ПО РЕКОМЕНДАЦИЯМ")
        report.append("=" * 60)
        report.append("")
        
        # Общая статистика
        total_recommendations = recommendations.height
        unique_users = recommendations['user_id'].unique().length()
        total_users = user_profiles.height
        
        report.append("📈 ОБЩАЯ СТАТИСТИКА:")
        report.append(f"  • Всего рекомендаций: {total_recommendations}")
        report.append(f"  • Уникальных пользователей: {unique_users}")
        report.append(f"  • Покрытие: {(unique_users/total_users)*100:.1f}%")
        report.append(f"  • Среднее рекомендаций на пользователя: {total_recommendations/unique_users:.1f}")
        report.append("")
        
        # Топ рекомендаций по продуктам
        product_stats = (
            recommendations.group_by('product_name')
            .agg([
                pl.count().alias('count'),
                pl.col('final_score').mean().alias('avg_score')
            ])
            .sort('count', descending=True)
            .head(5)
        )
        
        report.append("🏆 ТОП-5 РЕКОМЕНДУЕМЫХ ПРОДУКТОВ:")
        for row in product_stats.iter_rows(named=True):
            report.append(f"  • {row['product_name']}: {row['count']} рекомендаций (score: {row['avg_score']:.3f})")
        report.append("")
        
        # Анализ воздействия
        impact = impact_analysis['total_impact']
        report.append("💸 ОЦЕНКА ВОЗДЕЙСТВИЯ:")
        report.append(f"  • Оценка общей выручки: {impact['estimated_total_revenue']:,.0f}₽")
        report.append(f"  • Уровень уверенности: {impact['confidence_level']}")
        report.append("")
        
        # Распределение по сегментам пользователей
        segments = impact_analysis['user_segments']
        report.append("👥 РАСПРЕДЕЛЕНИЕ ПО СЕГМЕНТАМ:")
        for segment, stats in segments.items():
            report.append(f"  • {segment}: {stats['count']} пользователей ({stats['percentage']*100:.1f}%)")
        report.append("")
        
        # Качество рекомендаций
        avg_final_score = recommendations['final_score'].mean()
        high_quality_recs = recommendations.filter(pl.col('final_score') > 0.7).height
        high_quality_ratio = high_quality_recs / total_recommendations
        
        report.append("🎯 КАЧЕСТВО РЕКОМЕНДАЦИЙ:")
        report.append(f"  • Средний score: {avg_final_score:.3f}")
        report.append(f"  • Высококачественные рекомендации: {high_quality_ratio*100:.1f}%")
        report.append(f"  • Всего высококачественных: {high_quality_recs}")
        
        return "\n".join(report)
    
    def save_recommendation_analysis(self, recommendations: pl.DataFrame,
                                   user_profiles: pl.DataFrame,
                                   impact_analysis: Dict,
                                   filepath: str):
        """Сохранение полного анализа рекомендаций"""
        analysis_data = {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_recommendations': recommendations.height,
                'unique_users': recommendations['user_id'].unique().length(),
                'total_users': user_profiles.height,
                'avg_final_score': float(recommendations['final_score'].mean()),
                'total_estimated_revenue': impact_analysis['total_impact']['estimated_total_revenue']
            },
            'recommendations_sample': recommendations.head(100).to_dicts(),
            'impact_analysis': impact_analysis,
            'user_segments': impact_analysis['user_segments'],
            'product_impact': impact_analysis['product_impact']
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(analysis_data, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"💾 Анализ рекомендаций сохранен: {filepath}")
    
    def run_complete_analysis(self, users_df: pl.DataFrame,
                            events_df: pl.DataFrame,
                            items_df: pl.DataFrame,
                            output_dir: str = "results") -> Dict:
        """Запуск полного анализа рекомендационной системы"""
        print("🚀 ЗАПУСК ПОЛНОГО АНАЛИЗА РЕКОМЕНДАТЕЛЬНОЙ СИСТЕМЫ")
        print("=" * 60)
        
        # Создание директории для результатов
        Path(output_dir).mkdir(exist_ok=True)
        
        results = {}
        
        # 1. Сравнение стратегий
        print("\n1. 🔍 СРАВНЕНИЕ СТРАТЕГИЙ ОПТИМИЗАЦИИ")
        strategy_comparison = self.generate_strategy_comparison(users_df, events_df, items_df)
        results['strategy_comparison'] = strategy_comparison
        
        best_strategy = strategy_comparison['best_strategy']['strategy']
        print(f"   ✅ Лучшая стратегия: {best_strategy}")
        
        # 2. Генерация рекомендаций с лучшей стратегией
        print(f"\n2. 🎯 ГЕНЕРАЦИЯ РЕКОМЕНДАЦИЙ ({best_strategy} стратегия)")
        recommendations = self.generate_recommendations(
            users_df, events_df, items_df, best_strategy
        )
        results['recommendations'] = recommendations
        
        # 3. Создание пользовательских профилей
        print(f"\n3. 👤 СОЗДАНИЕ ПРОФИЛЕЙ ПОЛЬЗОВАТЕЛЕЙ")
        user_profiles = self.user_profiler.create_user_profiles(users_df, events_df, items_df)
        results['user_profiles'] = user_profiles
        
        # 4. Анализ воздействия
        print(f"\n4. 📊 АНАЛИЗ ВОЗДЕЙСТВИЯ РЕКОМЕНДАЦИЙ")
        impact_analysis = self.analyze_recommendation_impact(recommendations, user_profiles)
        results['impact_analysis'] = impact_analysis
        
        # 5. Расчет метрик
        print(f"\n5. 📈 РАСЧЕТ МЕТРИК КАЧЕСТВА")
        metrics = self.metrics_calculator.calculate_all_metrics(recommendations, user_profiles)
        results['metrics'] = metrics
        
        # 6. Генерация объяснений
        print(f"\n6. 📝 ГЕНЕРАЦИЯ ОБЪЯСНЕНИЙ")
        explanations = self.generate_personalized_explanations(recommendations, user_profiles)
        results['explanations'] = explanations
        
        # 7. Создание отчетов
        print(f"\n7. 📄 СОЗДАНИЕ ОТЧЕТОВ")
        report = self.create_recommendation_report(recommendations, user_profiles, impact_analysis)
        metrics_report = self.metrics_calculator.generate_metrics_report(metrics)
        
        # Сохранение результатов
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Сохранение данных
        self.save_recommendation_analysis(
            recommendations, user_profiles, impact_analysis,
            f"{output_dir}/recommendation_analysis_{timestamp}.json"
        )
        
        # Сохранение отчетов
        with open(f"{output_dir}/executive_report_{timestamp}.txt", 'w', encoding='utf-8') as f:
            f.write(report)
        
        with open(f"{output_dir}/metrics_report_{timestamp}.txt", 'w', encoding='utf-8') as f:
            f.write(metrics_report)
        
        # Сохранение рекомендаций
        recommendations.write_parquet(f"{output_dir}/recommendations_{timestamp}.parquet")
        user_profiles.write_parquet(f"{output_dir}/user_profiles_{timestamp}.parquet")
        explanations.write_parquet(f"{output_dir}/explanations_{timestamp}.parquet")
        
        print(f"\n✅ ПОЛНЫЙ АНАЛИЗ ЗАВЕРШЕН!")
        print(f"📁 Результаты сохранены в: {output_dir}/")
        print(f"📊 Отчеты: executive_report_{timestamp}.txt, metrics_report_{timestamp}.txt")
        
        return results