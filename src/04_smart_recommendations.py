# src/04_smart_recommendations.py
import pandas as pd
import numpy as np
from bank_products import BANK_PRODUCTS

class SmartRecommendationEngine:
    def __init__(self):
        self.products = BANK_PRODUCTS
    
    def calculate_product_score(self, user_features, product_id):
        """Рассчитываем релевантность продукта для пользователя"""
        product = self.products[product_id]
        score = 0
        
        # Базовые правила рекомендаций
        if product['category'] == 'credit':
            score += self.calculate_credit_score(user_features)
        
        elif product['category'] == 'savings':
            score += self.calculate_savings_score(user_features)
            
        elif product['category'] == 'premium':
            score += self.calculate_premium_score(user_features)
            
        elif product['category'] == 'credit_card':
            score += self.calculate_credit_card_score(user_features)
        
        # Специфические правила для каждого продукта
        if product_id == 'consumer_loan':
            if user_features.get('market_events', 0) > 50:
                score += 30
            if user_features.get('big_purchases', False):
                score += 40
                
        elif product_id == 'mortgage':
            if user_features.get('home_interest', 0) > 0.6:
                score += 50
            if user_features.get('family_products', False):
                score += 30
                
        elif product_id == 'premium_card':
            if user_features.get('premium_score', 0) > 0.7:
                score += 60
            if user_features.get('frequent_travel', False):
                score += 30
                
        elif product_id == 'sports_card':
            if user_features.get('sports_interest', 0) > 0.4:
                score += 70
            if user_features.get('healthy_lifestyle', False):
                score += 20
        
        return min(100, score)
    
    def calculate_credit_score(self, user_features):
        """Скоринг для кредитных продуктов"""
        score = 0
        
        # Активность
        if user_features.get('market_events', 0) > 50:
            score += 20
        
        # Вовлеченность с банком
        if user_features.get('offers_engagement', 0) > 5:
            score += 25
            
        # Стабильность поведения
        if user_features.get('consistent_activity', False):
            score += 15
            
        return score
    
    def calculate_savings_score(self, user_features):
        """Скоринг для сберегательных продуктов"""
        score = 0
        
        # Умеренная активность
        if 20 <= user_features.get('market_events', 0) <= 100:
            score += 30
            
        # Низкая вовлеченность с рисковыми продуктами
        if user_features.get('offers_engagement', 0) < 3:
            score += 25
            
        # Стабильное поведение
        if user_features.get('engagement_ratio', 0) < 0.1:
            score += 20
            
        return score
    
    def calculate_premium_score(self, user_features):
        """Скоринг для премиум-продуктов"""
        score = 0
        
        # Высокая активность
        if user_features.get('market_events', 0) > 100:
            score += 30
            
        # Интересы в премиум-сегменте
        if user_features.get('premium_score', 0) > 0.6:
            score += 35
            
        # Частые покупки
        if user_features.get('frequent_purchases', False):
            score += 25
            
        return score
    
    def recommend_products(self, user_features, top_n=5):
        """Генерируем персонализированные рекомендации"""
        scores = {}
        
        for product_id in self.products:
            score = self.calculate_product_score(user_features, product_id)
            scores[product_id] = score
        
        # Сортируем по убыванию релевантности
        sorted_products = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        
        recommendations = []
        for product_id, score in sorted_products[:top_n]:
            if score > 10:  # Минимальный порог релевантности
                product = self.products[product_id]
                recommendations.append({
                    'product_id': product_id,
                    'name': product['name'],
                    'category': product['category'],
                    'description': product['description'],
                    'score': score,
                    'match_percentage': f"{score}%"
                })
        
        return recommendations
    
    def generate_explanation(self, user_features, product):
        """Генерируем объяснение рекомендации"""
        explanations = []
        
        if product['category'] == 'credit':
            if user_features.get('market_events', 0) > 50:
                explanations.append("высокая активность в маркетплейсе")
            if user_features.get('offers_engagement', 0) > 5:
                explanations.append("активное взаимодействие с предложениями банка")
                
        elif product['category'] == 'savings':
            if user_features.get('engagement_ratio', 0) < 0.1:
                explanations.append("стабильное финансовое поведение")
            if 20 <= user_features.get('market_events', 0) <= 100:
                explanations.append("умеренная покупательская активность")
                
        elif product['category'] == 'premium':
            if user_features.get('premium_score', 0) > 0.6:
                explanations.append("интерес к премиум-товарам")
            if user_features.get('market_events', 0) > 100:
                explanations.append("высокая покупательская активность")
        
        if explanations:
            return f"Рекомендуем {product['name']}, потому что у вас: {', '.join(explanations)}"
        else:
            return f"Рекомендуем {product['name']} на основе вашего поведения"