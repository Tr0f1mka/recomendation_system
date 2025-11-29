import polars as pl
import numpy as np
from typing import Dict, List, Tuple
from datetime import datetime
import json

class RecommendationMetrics:
    """Класс для расчета метрик качества рекомендаций"""
    
    def __init__(self):
        self.metrics_history = []
    
    def calculate_all_metrics(self, recommendations: pl.DataFrame, 
                            user_profiles: pl.DataFrame,
                            test_events: pl.DataFrame = None) -> Dict:
        """Расчет всех метрик качества"""
        print("📈 Расчет метрик качества рекомендаций...")
        
        metrics = {}
        
        try:
            # Базовые метрики покрытия
            metrics['coverage'] = self._calculate_coverage_metrics(recommendations, user_profiles)
            
            # Метрики релевантности
            metrics['relevance'] = self._calculate_relevance_metrics(recommendations)
            
            # Метрики диверсификации
            metrics['diversity'] = self._calculate_diversity_metrics(recommendations)
            
            # Бизнес-метрики
            metrics['business'] = self._calculate_business_metrics(recommendations)
            
            # Метрики распределения
            metrics['distribution'] = self._calculate_distribution_metrics(recommendations)
            
            # Общий score
            metrics['overall_score'] = self._calculate_overall_score(metrics)
            
        except Exception as e:
            print(f"❌ Ошибка расчета метрик: {e}")
            # Возвращаем базовые метрики в случае ошибки
            metrics = self._get_basic_metrics(recommendations, user_profiles)
        
        return metrics
    
    def _get_basic_metrics(self, recommendations: pl.DataFrame, user_profiles: pl.DataFrame) -> Dict:
        """Базовые метрики в случае ошибки"""
        unique_users = recommendations['user_id'].unique().len() if recommendations.height > 0 else 0
        total_users = user_profiles.height
        
        return {
            'coverage': {
                'user_coverage_rate': unique_users / total_users if total_users > 0 else 0,
                'total_users_covered': unique_users
            },
            'relevance': {
                'avg_match_score': 0.5,
                'high_confidence_rate': 0
            },
            'diversity': {
                'product_diversity_score': 0,
                'diversity_index': 0
            },
            'business': {
                'avg_business_value_per_rec': 0.5
            },
            'distribution': {
                'score_distribution': {'mean': 0.5},
                'user_distribution': {'avg_recs_per_user': 0}
            },
            'overall_score': {
                'overall_score': 0.5,
                'quality_rating': 'fair'
            }
        }
    
    def _calculate_coverage_metrics(self, recommendations: pl.DataFrame, 
                                  user_profiles: pl.DataFrame) -> Dict:
        """Метрики покрытия пользователей"""
        if recommendations.height == 0:
            return {
                'user_coverage_rate': 0,
                'total_users_covered': 0,
                'avg_recommendations_per_user': 0,
                'users_with_multiple_recommendations': 0,
                'coverage_quality': 'low'
            }
        
        total_users = user_profiles.height
        users_with_recs = recommendations['user_id'].unique().len()
        
        coverage_rate = users_with_recs / total_users if total_users > 0 else 0
        
        # Распределение рекомендаций по пользователям
        recs_per_user = (
            recommendations.group_by('user_id')
            .agg(pl.count().alias('rec_count'))
        )
        
        avg_recs_per_user = recs_per_user['rec_count'].mean() if recs_per_user.height > 0 else 0
        users_with_multiple_recs = recs_per_user.filter(pl.col('rec_count') > 1).height
        
        return {
            'user_coverage_rate': round(coverage_rate, 3),
            'total_users_covered': users_with_recs,
            'avg_recommendations_per_user': round(float(avg_recs_per_user), 2),
            'users_with_multiple_recommendations': users_with_multiple_recs,
            'coverage_quality': 'high' if coverage_rate > 0.7 else 'medium' if coverage_rate > 0.4 else 'low'
        }
    
    def _calculate_relevance_metrics(self, recommendations: pl.DataFrame) -> Dict:
        """Метрики релевантности рекомендаций"""
        if recommendations.height == 0:
            return {
                'avg_match_score': 0,
                'avg_final_score': 0,
                'high_confidence_rate': 0,
                'confidence_distribution': {},
                'relevance_quality': 'low'
            }
        
        # Используем правильные колонки с fallback
        match_scores = recommendations['match_score'] if 'match_score' in recommendations.columns else pl.Series([0.5] * recommendations.height)
        final_scores = recommendations['final_score'] if 'final_score' in recommendations.columns else match_scores
        
        avg_match = float(match_scores.mean())
        avg_final = float(final_scores.mean())
        
        # Процент высокоуверенных рекомендаций
        if 'match_score' in recommendations.columns:
            high_confidence = recommendations.filter(pl.col('match_score') > 0.7).height
            high_confidence_rate = high_confidence / recommendations.height
            
            # Распределение по уровням уверенности
            confidence_levels = {
                'very_high': recommendations.filter(pl.col('match_score') > 0.8).height,
                'high': recommendations.filter((pl.col('match_score') > 0.6) & (pl.col('match_score') <= 0.8)).height,
                'medium': recommendations.filter((pl.col('match_score') > 0.4) & (pl.col('match_score') <= 0.6)).height,
                'low': recommendations.filter(pl.col('match_score') <= 0.4).height
            }
        else:
            high_confidence_rate = 0
            confidence_levels = {'very_high': 0, 'high': 0, 'medium': 0, 'low': recommendations.height}
        
        return {
            'avg_match_score': round(avg_match, 3),
            'avg_final_score': round(avg_final, 3),
            'high_confidence_rate': round(high_confidence_rate, 3),
            'confidence_distribution': confidence_levels,
            'relevance_quality': 'high' if avg_match > 0.6 else 'medium' if avg_match > 0.4 else 'low'
        }
    
    def _calculate_diversity_metrics(self, recommendations: pl.DataFrame) -> Dict:
        """Метрики диверсификации рекомендаций"""
        if recommendations.height == 0:
            return {
                'product_diversity_score': 0,
                'diversity_index': 0,
                'unique_products_recommended': 0,
                'product_type_distribution': [],
                'diversity_quality': 'low'
            }
        
        # Диверсификация по продуктам
        unique_products = recommendations['product_id'].unique().len() if 'product_id' in recommendations.columns else 0
        total_recommendations = recommendations.height
        product_diversity = unique_products / total_recommendations if total_recommendations > 0 else 0
        
        # Диверсификация по типам продуктов
        product_type_distribution = []
        diversity_index = 0
        
        if 'product_type' in recommendations.columns:
            product_type_dist = (
                recommendations.group_by('product_type')
                .agg(pl.count().alias('count'))
                .with_columns((pl.col('count') / total_recommendations).alias('percentage'))
            )
            
            # ИСПРАВЛЕНИЕ: правильная проверка DataFrame
            if product_type_dist.height > 0:  # Используем .height вместо проверки bool
                product_type_distribution = product_type_dist.to_dicts()
                
                # Индекс диверсификации (1 - концентрация)
                type_counts = product_type_dist['count'].to_list()
                concentration = sum((count / total_recommendations) ** 2 for count in type_counts)
                diversity_index = 1 - concentration
        
        return {
            'unique_products_recommended': unique_products,
            'product_diversity_score': round(product_diversity, 3),
            'diversity_index': round(diversity_index, 3),
            'product_type_distribution': product_type_distribution,
            'diversity_quality': 'high' if diversity_index > 0.7 else 'medium' if diversity_index > 0.4 else 'low'
        }
    
    def _calculate_business_metrics(self, recommendations: pl.DataFrame) -> Dict:
        """Бизнес-метрики рекомендаций"""
        if recommendations.height == 0:
            return {
                'total_business_value': 0,
                'avg_business_value_per_rec': 0,
                'premium_recommendations_rate': 0,
                'expected_conversion_rate': 0,
                'estimated_revenue_impact': {'estimated_total_impact': 0},
                'business_impact': 'low'
            }
        
        # Используем правильные колонки с fallback
        business_values = recommendations['business_value'] if 'business_value' in recommendations.columns else pl.Series([0.5] * recommendations.height)
        final_scores = recommendations['final_score'] if 'final_score' in recommendations.columns else business_values
        
        # Общая бизнес-ценность
        total_business_value = float((business_values * final_scores).sum())
        avg_business_value = float((business_values * final_scores).mean())
        
        # Распределение по ценовым сегментам продуктов
        if 'business_value' in recommendations.columns:
            premium_recommendations = recommendations.filter(pl.col('business_value') > 0.8).height
            premium_rate = premium_recommendations / recommendations.height
        else:
            premium_rate = 0
        
        # Ожидаемая конверсия (упрощенная модель)
        expected_conversion = float((final_scores * 0.3).mean())  # 30% базовый rate
        
        return {
            'total_business_value': round(total_business_value, 3),
            'avg_business_value_per_rec': round(avg_business_value, 3),
            'premium_recommendations_rate': round(premium_rate, 3),
            'expected_conversion_rate': round(expected_conversion, 3),
            'estimated_revenue_impact': self._estimate_revenue_impact(recommendations),
            'business_impact': 'high' if avg_business_value > 0.6 else 'medium' if avg_business_value > 0.4 else 'low'
        }
    
    def _estimate_revenue_impact(self, recommendations: pl.DataFrame) -> Dict:
        """Оценка потенциального влияния на выручку"""
        if recommendations.height == 0:
            return {'estimated_total_impact': 0, 'estimated_impact_per_user': 0, 'impact_confidence': 'low'}
        
        # Упрощенная модель оценки выручки
        if 'final_score' in recommendations.columns:
            high_value_recs = recommendations.filter(pl.col('final_score') > 0.6).height
            medium_value_recs = recommendations.filter((pl.col('final_score') > 0.4) & (pl.col('final_score') <= 0.6)).height
        else:
            high_value_recs = 0
            medium_value_recs = recommendations.height // 2
        
        # Предполагаемая ценность конверсии по сегментам
        premium_value = 50000  # Средняя ценность премиального продукта
        standard_value = 15000  # Средняя ценность стандартного продукта
        
        estimated_impact = (
            high_value_recs * premium_value * 0.3 +  # 30% конверсия для высоких scores
            medium_value_recs * standard_value * 0.15  # 15% конверсия для средних
        )
        
        unique_users = recommendations['user_id'].unique().len()
        impact_per_user = estimated_impact / unique_users if unique_users > 0 else 0
        
        return {
            'estimated_total_impact': round(estimated_impact, 2),
            'estimated_impact_per_user': round(impact_per_user, 2),
            'impact_confidence': 'high' if high_value_recs > medium_value_recs else 'medium'
        }
    
    def _calculate_distribution_metrics(self, recommendations: pl.DataFrame) -> Dict:
        """Метрики распределения рекомендаций"""
        if recommendations.height == 0:
            return {'score_distribution': {}, 'user_distribution': {}}
        
        # Распределение scores
        score_stats = {'mean': 0, 'std': 0, 'min': 0, 'max': 0, 'q25': 0, 'q75': 0}
        if 'final_score' in recommendations.columns:
            score_data = recommendations['final_score']
            score_stats = {
                'mean': float(score_data.mean()),
                'std': float(score_data.std()),
                'min': float(score_data.min()),
                'max': float(score_data.max()),
                'q25': float(score_data.quantile(0.25)),
                'q75': float(score_data.quantile(0.75))
            }
        
        # Распределение по пользователям
        user_rec_counts = (
            recommendations.group_by('user_id')
            .agg(pl.count().alias('rec_count'))
        )
        
        if user_rec_counts.height > 0:
            rec_count_data = user_rec_counts['rec_count']
            user_dist_stats = {
                'mean': float(rec_count_data.mean()),
                'std': float(rec_count_data.std()),
                'min': float(rec_count_data.min()),
                'max': float(rec_count_data.max())
            }
            
            users_with_1_rec = user_rec_counts.filter(pl.col('rec_count') == 1).height
            users_with_3plus_recs = user_rec_counts.filter(pl.col('rec_count') >= 3).height
        else:
            user_dist_stats = {'mean': 0, 'std': 0, 'min': 0, 'max': 0}
            users_with_1_rec = 0
            users_with_3plus_recs = 0
        
        return {
            'score_distribution': {
                'mean': round(score_stats['mean'], 3),
                'std': round(score_stats['std'], 3),
                'min': round(score_stats['min'], 3),
                'max': round(score_stats['max'], 3),
                'q25': round(score_stats['q25'], 3),
                'q75': round(score_stats['q75'], 3)
            },
            'user_distribution': {
                'avg_recs_per_user': round(user_dist_stats['mean'], 2),
                'max_recs_to_user': user_dist_stats['max'],
                'users_with_1_rec': users_with_1_rec,
                'users_with_3plus_recs': users_with_3plus_recs
            }
        }
    
    def _calculate_overall_score(self, metrics: Dict) -> Dict:
        """Расчет общего score системы"""
        weights = {
            'coverage': 0.25,
            'relevance': 0.35, 
            'diversity': 0.20,
            'business': 0.20
        }
        
        # Нормализация метрик
        coverage_score = metrics['coverage']['user_coverage_rate']
        relevance_score = metrics['relevance']['avg_match_score']
        diversity_score = metrics['diversity']['diversity_index']
        business_score = min(metrics['business']['avg_business_value_per_rec'] * 2, 1.0)
        
        overall_score = (
            coverage_score * weights['coverage'] +
            relevance_score * weights['relevance'] + 
            diversity_score * weights['diversity'] +
            business_score * weights['business']
        )
        
        # Качественная оценка
        if overall_score > 0.7:
            quality = 'excellent'
        elif overall_score > 0.5:
            quality = 'good'
        elif overall_score > 0.3:
            quality = 'fair'
        else:
            quality = 'poor'
        
        return {
            'overall_score': round(overall_score, 3),
            'quality_rating': quality,
            'component_scores': {
                'coverage': round(coverage_score, 3),
                'relevance': round(relevance_score, 3),
                'diversity': round(diversity_score, 3),
                'business': round(business_score, 3)
            }
        }
    
    def generate_metrics_report(self, metrics: Dict) -> str:
        """Генерация текстового отчета по метрикам"""
        report = []
        report.append("📊 ОТЧЕТ ПО МЕТРИКАМ РЕКОМЕНДАТЕЛЬНОЙ СИСТЕМЫ")
        report.append("=" * 50)
        
        # Общая оценка
        overall = metrics['overall_score']
        report.append(f"🏆 ОБЩАЯ ОЦЕНКА: {overall['overall_score']} ({overall['quality_rating']})")
        report.append("")
        
        # Детальные метрики
        report.append("📈 ДЕТАЛЬНЫЕ МЕТРИКИ:")
        report.append(f"  👥 Покрытие пользователей: {metrics['coverage']['user_coverage_rate'] * 100:.1f}%")
        report.append(f"  🎯 Релевантность: {metrics['relevance']['avg_match_score']}")
        report.append(f"  🌈 Диверсификация: {metrics['diversity']['diversity_index']}")
        report.append(f"  💰 Бизнес-ценность: {metrics['business']['avg_business_value_per_rec']}")
        report.append("")
        
        # Бизнес-метрики
        business = metrics['business']['estimated_revenue_impact']
        report.append("💸 БИЗНЕС-ВОЗДЕЙСТВИЕ:")
        report.append(f"  Оценка влияния на выручку: {business['estimated_total_impact']:,.0f}₽")
        report.append(f"  На пользователя: {business['estimated_impact_per_user']:,.0f}₽")
        report.append(f"  Доверие оценки: {business['impact_confidence']}")
        
        return "\n".join(report)
    
    def save_metrics(self, metrics: Dict, filepath: str):
        """Сохранение метрик в файл"""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(metrics, f, ensure_ascii=False, indent=2, default=str)