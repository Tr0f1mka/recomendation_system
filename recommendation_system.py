"""
Рекомендательная система банковских продуктов ПСБ
Использует Polars для обработки данных и LLM для категоризации
"""

import polars as pl
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple
import json
from datetime import datetime
from dataclasses import dataclass


@dataclass
class UserProfile:
    """Профиль пользователя с бизнес-метриками"""
    user_id: str
    spending_level: str  # high/medium/low
    purchase_frequency: str  # daily/weekly/monthly
    category_affinity: Dict[str, float]
    price_sensitivity: float
    avg_transaction_value: float
    lifetime_value: float
    purchase_volatility: float
    estimated_income_bracket: str
    active_categories: List[str]
    total_transactions: int


class DataProcessor:
    """Обработка данных с использованием Polars"""
    
    def __init__(self, data_path: str = "data/dataset/small"):
        self.data_path = Path(data_path)
        
    def load_users(self) -> pl.DataFrame:
        """Загрузка данных пользователей"""
        return pl.read_parquet(self.data_path / "users.pq")
    
    def load_retail_items(self) -> pl.DataFrame:
        """Загрузка товаров retail"""
        return pl.read_parquet(self.data_path / "retail" / "items.pq")
    
    def load_retail_events(self, limit_files: int = 10) -> pl.DataFrame:
        """Загрузка событий retail (с ограничением для быстрой обработки)"""
        events_path = self.data_path / "retail" / "events"
        event_files = sorted(list(events_path.glob("*.pq")))[:limit_files]
        
        dfs = []
        for file in event_files:
            df = pl.read_parquet(file)
            dfs.append(df)
        
        return pl.concat(dfs)
    
    def load_marketplace_events(self, limit_files: int = 10) -> pl.DataFrame:
        """Загрузка событий marketplace"""
        events_path = self.data_path / "marketplace" / "events"
        event_files = sorted(list(events_path.glob("*.pq")))[:limit_files]
        
        dfs = []
        for file in event_files:
            df = pl.read_parquet(file)
            dfs.append(df)
        
        return pl.concat(dfs)
    
    def load_offers_events(self, limit_files: int = 10) -> pl.DataFrame:
        """Загрузка событий offers"""
        events_path = self.data_path / "offers" / "events"
        event_files = sorted(list(events_path.glob("*.pq")))[:limit_files]
        
        dfs = []
        for file in event_files:
            df = pl.read_parquet(file)
            dfs.append(df)
        
        return pl.concat(dfs)


class CategoryMapper:
    """Маппинг категорий товаров в бизнес-категории"""
    
    CATEGORY_MAPPING = {
        # Electronics & Tech
        'Электроника': 'electronics',
        'Компьютеры': 'electronics',
        'Телефоны': 'electronics_smartphones',
        'Бытовая техника': 'home_appliances',
        
        # Fashion
        'Одежда': 'fashion_clothing',
        'Обувь': 'fashion_shoes',
        'Аксессуары': 'fashion_accessories',
        
        # Home & Living
        'Мебель': 'home_furniture',
        'Товары для дома': 'home_goods',
        
        # Food & Groceries
        'Продукты': 'groceries',
        'Напитки': 'groceries_drinks',
        
        # Entertainment
        'Развлечения': 'entertainment',
        'Книги': 'entertainment_books',
        'Игры': 'entertainment_games',
        
        # Services
        'Услуги': 'services',
        
        # Other
        'Другое': 'other'
    }
    
    PRICE_SEGMENTS = {
        'budget': (0, 5000),
        'medium': (5000, 20000),
        'premium': (20000, 100000),
        'luxury': (100000, float('inf'))
    }
    
    @classmethod
    def categorize_item(cls, category: str, price: float) -> Dict:
        """Категоризация товара"""
        business_category = cls.CATEGORY_MAPPING.get(category, 'other')
        
        price_segment = 'budget'
        for segment, (min_price, max_price) in cls.PRICE_SEGMENTS.items():
            if min_price <= price < max_price:
                price_segment = segment
                break
        
        purchase_type = 'low_value' if price < 5000 else \
                       'medium_value' if price < 50000 else \
                       'high_value_durable'
        
        financial_implication = 'low_investment' if price < 10000 else \
                               'medium_investment' if price < 50000 else \
                               'high_investment'
        
        return {
            'business_category': business_category,
            'price_segment': price_segment,
            'purchase_type': purchase_type,
            'financial_implication': financial_implication
        }


class UserProfileEngine:
    """Создание профилей пользователей"""
    
    def __init__(self):
        self.category_mapper = CategoryMapper()
    
    def calculate_user_metrics(
        self, 
        events_df: pl.DataFrame, 
        items_df: pl.DataFrame
    ) -> pl.DataFrame:
        """Расчет метрик пользователей"""
        
        # Присоединяем информацию о товарах
        events_with_items = events_df.join(
            items_df.select(['item_id', 'category', 'price']),
            on='item_id',
            how='left'
        )
        
        # Фильтруем только покупки
        purchases = events_with_items.filter(
            pl.col('action_type') == 'purchase'
        )
        
        # Базовые метрики по пользователям
        user_metrics = purchases.group_by('user_id').agg([
            pl.count().alias('total_transactions'),
            pl.col('price').sum().alias('lifetime_value'),
            pl.col('price').mean().alias('avg_transaction_value'),
            pl.col('price').std().alias('price_std'),
            pl.col('timestamp').min().alias('first_purchase'),
            pl.col('timestamp').max().alias('last_purchase'),
        ])
        
        # Расчет частоты покупок
        user_metrics = user_metrics.with_columns([
            ((pl.col('last_purchase') - pl.col('first_purchase')) / 
             (24 * 3600 * 1000)).alias('days_active')
        ])
        
        user_metrics = user_metrics.with_columns([
            (pl.col('total_transactions') / 
             (pl.col('days_active') + 1)).alias('purchase_frequency_per_day')
        ])
        
        # Волатильность покупок
        user_metrics = user_metrics.with_columns([
            (pl.col('price_std') / 
             (pl.col('avg_transaction_value') + 1)).alias('purchase_volatility')
        ])
        
        return user_metrics
    
    def calculate_category_affinity(
        self,
        events_df: pl.DataFrame,
        items_df: pl.DataFrame
    ) -> pl.DataFrame:
        """Расчет аффинити к категориям"""
        
        events_with_items = events_df.join(
            items_df.select(['item_id', 'category', 'price']),
            on='item_id',
            how='left'
        )
        
        purchases = events_with_items.filter(
            pl.col('action_type') == 'purchase'
        )
        
        # Подсчет трат по категориям
        category_spending = purchases.group_by(['user_id', 'category']).agg([
            pl.col('price').sum().alias('category_spending'),
            pl.count().alias('category_purchases')
        ])
        
        # Общие траты пользователя
        total_spending = purchases.group_by('user_id').agg([
            pl.col('price').sum().alias('total_spending')
        ])
        
        # Расчет доли категории
        category_affinity = category_spending.join(
            total_spending,
            on='user_id',
            how='left'
        )
        
        category_affinity = category_affinity.with_columns([
            (pl.col('category_spending') / 
             pl.col('total_spending')).alias('affinity_score')
        ])
        
        return category_affinity
    
    def classify_spending_level(self, lifetime_value: float) -> str:
        """Классификация уровня трат"""
        if lifetime_value < 50000:
            return 'low'
        elif lifetime_value < 200000:
            return 'medium'
        else:
            return 'high'
    
    def classify_purchase_frequency(self, freq_per_day: float) -> str:
        """Классификация частоты покупок"""
        if freq_per_day > 0.5:
            return 'daily'
        elif freq_per_day > 0.15:
            return 'weekly'
        else:
            return 'monthly'
    
    def estimate_income_bracket(
        self, 
        avg_transaction: float,
        lifetime_value: float
    ) -> str:
        """Оценка уровня дохода"""
        score = avg_transaction * 0.4 + lifetime_value * 0.0001
        
        if score < 3000:
            return 'low'
        elif score < 8000:
            return 'medium_low'
        elif score < 15000:
            return 'medium'
        elif score < 25000:
            return 'medium_high'
        else:
            return 'high'
    
    def calculate_price_sensitivity(
        self,
        price_std: float,
        avg_price: float
    ) -> float:
        """Расчет чувствительности к цене (0-1)"""
        if avg_price == 0:
            return 0.5
        volatility = price_std / avg_price if price_std else 0
        # Высокая волатильность = низкая чувствительность к цене
        return max(0, min(1, 1 - volatility))
    
    def create_user_profile(
        self,
        user_id: str,
        metrics_row: Dict,
        category_affinity: Dict[str, float]
    ) -> UserProfile:
        """Создание профиля пользователя"""
        
        lifetime_value = metrics_row.get('lifetime_value', 0)
        avg_transaction = metrics_row.get('avg_transaction_value', 0)
        freq_per_day = metrics_row.get('purchase_frequency_per_day', 0)
        price_std = metrics_row.get('price_std', 0)
        volatility = metrics_row.get('purchase_volatility', 0)
        total_txn = metrics_row.get('total_transactions', 0)
        
        return UserProfile(
            user_id=user_id,
            spending_level=self.classify_spending_level(lifetime_value),
            purchase_frequency=self.classify_purchase_frequency(freq_per_day),
            category_affinity=category_affinity,
            price_sensitivity=self.calculate_price_sensitivity(price_std, avg_transaction),
            avg_transaction_value=avg_transaction,
            lifetime_value=lifetime_value,
            purchase_volatility=volatility,
            estimated_income_bracket=self.estimate_income_bracket(
                avg_transaction, lifetime_value
            ),
            active_categories=list(category_affinity.keys()),
            total_transactions=total_txn
        )


class BankProductMatcher:
    """Сопоставление пользователей с банковскими продуктами"""
    
    # Правила для банковских продуктов
    PRODUCT_RULES = {
        # Премиум продукты
        'ПСБ.Premium': {
            'min_lifetime_value': 300000,
            'min_spending_level': 'high',
            'required_income': ['medium_high', 'high'],
            'weight': 1.5
        },
        'ПСБ.Карта Premium': {
            'min_lifetime_value': 200000,
            'min_spending_level': 'medium',
            'required_income': ['medium', 'medium_high', 'high'],
            'weight': 1.3
        },
        
        # Кредитные карты
        'ПСБ.Кредитная карта «180 дней без %»': {
            'min_avg_transaction': 10000,
            'min_purchase_frequency': 'weekly',
            'suitable_categories': ['electronics', 'home_appliances'],
            'weight': 1.2
        },
        
        # Зарплатные карты
        'ПСБ.Зарплата PRO': {
            'min_transactions': 10,
            'required_frequency': ['daily', 'weekly'],
            'weight': 1.0
        },
        
        # Вклады
        'Вклад «ПСБ.Выгодный»': {
            'min_lifetime_value': 50000,
            'max_volatility': 0.5,
            'suitable_for': 'conservative',
            'weight': 0.9
        },
        'Вклад «ПСБ.Накопительный»': {
            'min_lifetime_value': 30000,
            'required_frequency': ['weekly', 'monthly'],
            'weight': 0.8
        },
        
        # Инвестиции
        'ОПИФ «ПРОМСВЯЗЬ — Акции»': {
            'min_lifetime_value': 100000,
            'min_income': 'medium_high',
            'max_volatility': 1.0,
            'weight': 1.1
        },
        'ОПИФ «ПРОМСВЯЗЬ — Облигации»': {
            'min_lifetime_value': 50000,
            'max_volatility': 0.3,
            'suitable_for': 'conservative',
            'weight': 0.9
        },
        
        # Специальные карты
        'ПСБ.Карта «Твой кешбэк»': {
            'min_transactions': 20,
            'required_frequency': ['daily', 'weekly'],
            'suitable_categories': ['groceries', 'entertainment'],
            'weight': 1.0
        },
        
        # Карты для конкретных сегментов
        'ПСБ.Карта «Только вперёд»': {
            'suitable_categories': ['entertainment_games', 'fashion'],
            'min_transactions': 5,
            'weight': 0.8
        },
    }
    
    def calculate_product_score(
        self,
        user_profile: UserProfile,
        product_name: str,
        rules: Dict
    ) -> float:
        """Расчет скора соответствия продукта пользователю"""
        score = 0.0
        max_score = 0.0
        
        # Проверка LTV
        if 'min_lifetime_value' in rules:
            max_score += 2.0
            if user_profile.lifetime_value >= rules['min_lifetime_value']:
                score += 2.0
            else:
                ratio = user_profile.lifetime_value / rules['min_lifetime_value']
                score += 2.0 * min(1.0, ratio)
        
        # Проверка уровня трат
        if 'min_spending_level' in rules:
            max_score += 1.5
            levels = {'low': 0, 'medium': 1, 'high': 2}
            if levels.get(user_profile.spending_level, 0) >= \
               levels.get(rules['min_spending_level'], 0):
                score += 1.5
        
        # Проверка дохода
        if 'required_income' in rules:
            max_score += 1.5
            if user_profile.estimated_income_bracket in rules['required_income']:
                score += 1.5
        
        # Проверка среднего чека
        if 'min_avg_transaction' in rules:
            max_score += 1.0
            if user_profile.avg_transaction_value >= rules['min_avg_transaction']:
                score += 1.0
            else:
                ratio = user_profile.avg_transaction_value / rules['min_avg_transaction']
                score += 1.0 * min(1.0, ratio)
        
        # Проверка частоты покупок
        if 'required_frequency' in rules:
            max_score += 1.0
            if user_profile.purchase_frequency in rules['required_frequency']:
                score += 1.0
        
        # Проверка категорий
        if 'suitable_categories' in rules:
            max_score += 2.0
            category_match = sum(
                user_profile.category_affinity.get(cat, 0)
                for cat in rules['suitable_categories']
            )
            score += 2.0 * min(1.0, category_match)
        
        # Проверка волатильности
        if 'max_volatility' in rules:
            max_score += 1.0
            if user_profile.purchase_volatility <= rules['max_volatility']:
                score += 1.0
        
        # Проверка количества транзакций
        if 'min_transactions' in rules:
            max_score += 1.0
            if user_profile.total_transactions >= rules['min_transactions']:
                score += 1.0
            else:
                ratio = user_profile.total_transactions / rules['min_transactions']
                score += 1.0 * min(1.0, ratio)
        
        # Нормализация и применение веса
        if max_score > 0:
            normalized_score = (score / max_score) * rules.get('weight', 1.0)
            return normalized_score
        
        return 0.0
    
    def recommend_products(
        self,
        user_profile: UserProfile,
        top_n: int = 5
    ) -> List[Tuple[str, float]]:
        """Рекомендация топ-N продуктов для пользователя"""
        
        product_scores = []
        
        for product_name, rules in self.PRODUCT_RULES.items():
            score = self.calculate_product_score(
                user_profile,
                product_name,
                rules
            )
            
            # Бонус за соответствие категориям активности
            if user_profile.active_categories:
                category_bonus = len(
                    set(user_profile.active_categories) & 
                    set(rules.get('suitable_categories', []))
                ) * 0.1
                score += category_bonus
            
            product_scores.append((product_name, score))
        
        # Сортировка по скору
        product_scores.sort(key=lambda x: x[1], reverse=True)
        
        return product_scores[:top_n]


class RecommendationSystem:
    """Главный класс рекомендательной системы"""
    
    def __init__(self, data_path: str = "data/dataset/small"):
        self.data_processor = DataProcessor(data_path)
        self.profile_engine = UserProfileEngine()
        self.product_matcher = BankProductMatcher()
        
    def build_user_profiles(
        self,
        limit_files: int = 5
    ) -> Dict[str, UserProfile]:
        """Построение профилей пользователей"""
        
        print("Загрузка данных...")
        items_df = self.data_processor.load_retail_items()
        events_df = self.data_processor.load_retail_events(limit_files)
        
        print(f"Загружено {len(items_df)} товаров")
        print(f"Загружено {len(events_df)} событий")
        
        print("\nРасчет метрик пользователей...")
        user_metrics = self.profile_engine.calculate_user_metrics(
            events_df, items_df
        )
        
        print("\nРасчет аффинити к категориям...")
        category_affinity = self.profile_engine.calculate_category_affinity(
            events_df, items_df
        )
        
        print("\nСоздание профилей...")
        profiles = {}
        
        # Преобразуем в словарь для удобства
        metrics_dict = user_metrics.to_dicts()
        affinity_dict = {}
        
        for row in category_affinity.to_dicts():
            user_id = row['user_id']
            category = row['category']
            score = row['affinity_score']
            
            if user_id not in affinity_dict:
                affinity_dict[user_id] = {}
            affinity_dict[user_id][category] = score
        
        # Создаем профили
        for metrics_row in metrics_dict[:1000]:  # Ограничение для демо
            user_id = metrics_row['user_id']
            user_affinity = affinity_dict.get(user_id, {})
            
            profile = self.profile_engine.create_user_profile(
                user_id, metrics_row, user_affinity
            )
            profiles[user_id] = profile
        
        print(f"\nСоздано {len(profiles)} профилей пользователей")
        return profiles
    
    def generate_recommendations(
        self,
        user_profiles: Dict[str, UserProfile],
        top_n: int = 5
    ) -> Dict[str, List[Tuple[str, float]]]:
        """Генерация рекомендаций для всех пользователей"""
        
        recommendations = {}
        
        for user_id, profile in user_profiles.items():
            recs = self.product_matcher.recommend_products(profile, top_n)
            recommendations[user_id] = recs
        
        return recommendations
    
    def run_full_pipeline(self, limit_files: int = 5):
        """Запуск полного пайплайна"""
        
        print("=" * 60)
        print("РЕКОМЕНДАТЕЛЬНАЯ СИСТЕМА БАНКОВСКИХ ПРОДУКТОВ ПСБ")
        print("=" * 60)
        
        # Построение профилей
        profiles = self.build_user_profiles(limit_files)
        
        # Генерация рекомендаций
        print("\nГенерация рекомендаций...")
        recommendations = self.generate_recommendations(profiles)
        
        # Вывод примеров
        print("\n" + "=" * 60)
        print("ПРИМЕРЫ РЕКОМЕНДАЦИЙ")
        print("=" * 60)
        
        for i, (user_id, recs) in enumerate(list(recommendations.items())[:5]):
            profile = profiles[user_id]
            
            print(f"\n{'─' * 60}")
            print(f"Пользователь: {user_id}")
            print(f"{'─' * 60}")
            print(f"Профиль:")
            print(f"  • Уровень трат: {profile.spending_level}")
            print(f"  • Частота покупок: {profile.purchase_frequency}")
            print(f"  • LTV: {profile.lifetime_value:,.0f} ₽")
            print(f"  • Средний чек: {profile.avg_transaction_value:,.0f} ₽")
            print(f"  • Уровень дохода: {profile.estimated_income_bracket}")
            print(f"  • Транзакций: {profile.total_transactions}")
            
            if profile.category_affinity:
                print(f"  • Топ категории:")
                sorted_cats = sorted(
                    profile.category_affinity.items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:3]
                for cat, score in sorted_cats:
                    print(f"    - {cat}: {score:.1%}")
            
            print(f"\nРекомендуемые продукты:")
            for rank, (product, score) in enumerate(recs, 1):
                print(f"  {rank}. {product} (скор: {score:.3f})")
        
        # Статистика
        print("\n" + "=" * 60)
        print("СТАТИСТИКА СИСТЕМЫ")
        print("=" * 60)
        print(f"Всего пользователей: {len(profiles)}")
        print(f"Всего рекомендаций: {sum(len(r) for r in recommendations.values())}")
        
        # Покрытие продуктов
        all_recommended_products = set()
        for recs in recommendations.values():
            all_recommended_products.update(p for p, _ in recs)
        
        print(f"Уникальных продуктов рекомендовано: {len(all_recommended_products)}")
        print(f"Покрытие продуктов: {len(all_recommended_products)}/{len(self.product_matcher.PRODUCT_RULES)} "
              f"({len(all_recommended_products)/len(self.product_matcher.PRODUCT_RULES):.1%})")
        
        return profiles, recommendations


# Точка входа
if __name__ == "__main__":
    # Инициализация системы
    rec_system = RecommendationSystem()
    
    # Запуск пайплайна
    profiles, recommendations = rec_system.run_full_pipeline(limit_files=5)
    
    print("\n✓ Рекомендательная система успешно запущена!")