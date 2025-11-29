import polars as pl
from typing import Dict, List, Tuple
import numpy as np
from config.products import BANK_PRODUCTS
from src.llm_processor import FreeLLMProcessor
from src.ml_enhanced_matcher import MLEnhancer

class ProductMatcher:
    def __init__(self, discovered_categories: Dict):
        self.bank_products = BANK_PRODUCTS
        self.discovered_categories = discovered_categories
        self.llm_processor = FreeLLMProcessor(discovered_categories)
        self.ml_enhancer = MLEnhancer()
        
        # Пытаемся загрузить предобученную модель
        self.ml_enhancer.load_model()
        
    def match_users_to_products(self, user_profiles: pl.DataFrame, use_ml: bool = True) -> pl.DataFrame:
        """Сопоставление пользователей с продуктами с ML/LLM улучшением"""
        print("🎯 Сопоставление пользователей с продуктами (с ML/LLM)...")
        
        recommendations = []
        processed_users = 0
        
        for user_row in user_profiles.iter_rows(named=True):
            try:
                user_recs = self._get_enhanced_recommendations_for_user(user_row, use_ml)
                recommendations.extend(user_recs)
                
                processed_users += 1
                if processed_users % 100 == 0:
                    print(f"   Обработано {processed_users}/{user_profiles.height} пользователей")
            except Exception as e:
                print(f"   ⚠️ Ошибка обработки пользователя {user_row.get('user_id', 'unknown')}: {e}")
                continue
        
        if not recommendations:
            print("❌ Не удалось сгенерировать ни одной рекомендации")
            return pl.DataFrame()
            
        df = pl.DataFrame(recommendations)
        print(f"✅ Сгенерировано {df.height} рекомендаций для {processed_users} пользователей")
        return df
    
    def _get_enhanced_recommendations_for_user(self, user_profile: Dict, use_ml: bool) -> List[Dict]:
        """Улучшенные рекомендации для пользователя с ML/LLM"""
        recommendations = []
        
        print(f"      ML статус: use_ml={use_ml}, is_trained={self.ml_enhancer.is_trained}")  # ОТЛАДКА
        
        # Базовый расчет для всех продуктов
        for product_type, products in self.bank_products.items():
            for product in products:
                try:
                    # Базовый расчет соответствия
                    base_score, reasoning = self._calculate_simple_product_match(user_profile, product)
                    
                    # ML оптимизация если включена
                    if use_ml and self.ml_enhancer.is_trained:
                        try:
                            final_score = self.ml_enhancer.optimize(base_score, user_profile, product)
                            ml_used = True
                            print(f"      ✅ ML применен: {base_score:.3f} -> {final_score:.3f}")  # ОТЛАДКА
                        except Exception as e:
                            # Если ML не готов, используем базовый score
                            final_score = base_score
                            ml_used = False
                            print(f"      ⚠️ ML ошибка: {e}")  # ОТЛАДКА
                    else:
                        final_score = base_score
                        ml_used = False
                        if use_ml and not self.ml_enhancer.is_trained:
                            print(f"      ⚠️ ML не обучена")  # ОТЛАДКА
                    
                    # Генерация объяснения на основе данных пользователя
                    explanation = self._generate_data_based_explanation(user_profile, product, reasoning)
                    
                    if final_score > 0.2:  # Низкий порог для лучшего покрытия
                        recommendations.append({
                            'user_id': user_profile['user_id'],
                            'product_id': product['id'],
                            'product_name': product['name'],
                            'product_type': product_type,
                            'base_match_score': round(base_score, 3),
                            'final_score': round(final_score, 3),
                            'reasoning': "; ".join(reasoning),
                            'llm_explanation': explanation,
                            'business_value': product.get('business_value', 0.5),
                            'ml_enhanced': ml_used,  # ДОЛЖНО БЫТЬ True при использовании ML
                            'llm_enhanced': True
                        })
                except Exception as e:
                    print(f"      ⚠️ Ошибка обработки продукта {product.get('name', 'unknown')}: {e}")
                    continue
        
        # Сортируем по итоговому score и возвращаем топ-5
        return sorted(recommendations, key=lambda x: x['final_score'], reverse=True)[:5]

    def _calculate_simple_product_match(self, user_profile: Dict, product: Dict) -> Tuple[float, List[str]]:
        """Упрощенный и безопасный расчет соответствия"""
        reasoning_parts = []
        score = 0.3  # Базовый score для всех
        
        # Безопасное получение значений с дефолтами
        user_spending = user_profile.get('spending_level', 'unknown')
        user_activity = user_profile.get('interaction_frequency', 'unknown')
        user_total_spent = user_profile.get('total_spent', 0) or 0
        user_avg_transaction = user_profile.get('avg_transaction_value', 0) or 0
        user_diversity = user_profile.get('category_diversity', 0) or 0
        activity_duration = user_profile.get('activity_duration_days', 0) or 0
        
        # 1. Уровень трат
        if user_spending in ['high', 'very_high']:
            score += 0.2
            reasoning_parts.append("Высокий уровень трат")
        elif user_spending == 'medium':
            score += 0.1
            reasoning_parts.append("Средний уровень трат")
        
        # 2. Активность
        if user_activity in ['high', 'very_high']:
            score += 0.15
            reasoning_parts.append("Высокая активность")
        elif user_activity == 'medium':
            score += 0.08
            reasoning_parts.append("Умеренная активность")
        
        # 3. Общие траты
        if user_total_spent > 50000:
            score += 0.15
            reasoning_parts.append("Значительные общие траты")
        elif user_total_spent > 20000:
            score += 0.08
            reasoning_parts.append("Заметные траты")
        
        # 4. Средний чек
        if user_avg_transaction > 10000:
            score += 0.1
            reasoning_parts.append("Высокий средний чек")
        elif user_avg_transaction > 5000:
            score += 0.05
            reasoning_parts.append("Средний чек выше среднего")
        
        # 5. Разнообразие интересов
        if user_diversity > 0.3:
            score += 0.1
            reasoning_parts.append("Широкие интересы")
        elif user_diversity > 0.1:
            score += 0.05
            reasoning_parts.append("Разнообразные интересы")
        
        # 6. Длительность активности
        if activity_duration > 180:
            score += 0.08
            reasoning_parts.append("Длительная активность")
        elif activity_duration > 30:
            score += 0.04
            reasoning_parts.append("Стабильная активность")
        
        # 7. Категориальные предпочтения (если есть)
        category_affinity = user_profile.get('category_affinity', {}) or {}
        if category_affinity:
            score += 0.05
            reasoning_parts.append("Есть категориальные предпочтения")
        
        return min(score, 1.0), reasoning_parts
    
    def _generate_data_based_explanation(self, user_profile: Dict, product: Dict, reasoning: List[str]) -> str:
        """Генерация объяснения на основе реальных данных пользователя"""
        if not reasoning:
            return f"Рекомендуем {product['name']} на основе общего анализа вашего профиля."
        
        # Безопасное получение данных
        user_spending = user_profile.get('spending_level', 'средний')
        user_activity = user_profile.get('interaction_frequency', 'средняя')
        user_total_spent = user_profile.get('total_spent', 0) or 0
        
        explanation_parts = [f"Рекомендуем {product['name']} потому что:"]
        
        # Добавляем основные причины
        for reason in reasoning[:3]:
            explanation_parts.append(f"• {reason}")
        
        # Добавляем персонализацию
        if user_spending in ['high', 'very_high']:
            explanation_parts.append("• ваш уровень трат идеально подходит для этого продукта")
        
        if user_activity in ['high', 'very_high']:
            explanation_parts.append("• ваша активность показывает высокий потенциал")
            
        if user_total_spent > 50000:
            explanation_parts.append("• ваши значительные траты соответствуют премиальным продуктам")
        
        return " ".join(explanation_parts)
    
    def train_ml_model(self, training_data: List[Dict]):
        """Обучение ML модели на исторических данных"""
        print("🤖 Обучение ML модели на реальных данных...")
        try:
            self.ml_enhancer.train(training_data)  # вместо train_model
            print("✅ ML модель успешно обучена")
        except Exception as e:
            print(f"❌ Ошибка обучения ML модели: {e}")