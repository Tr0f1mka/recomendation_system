import polars as pl
import re
from typing import Dict, List, Tuple
from collections import Counter
import json
import numpy as np

class CategoryDiscoverer:
    def __init__(self):
        self.category_hierarchy = {}
        self.price_segments = {}
        
    def discover_categories_from_data(self, items_df: pl.DataFrame) -> Dict:
        """Автоматическое обнаружение категорий из данных"""
        print("🎯 Обнаружение категорий из данных...")
        
        categories = {}
        
        # Анализ существующих категорий
        if 'category' in items_df.columns:
            categories['existing_categories'] = self._analyze_existing_categories(items_df)
        
        # Анализ брендов
        if 'brand_id' in items_df.columns:
            categories['brands_analysis'] = self._analyze_brands(items_df)
            
        # Автоматическая категоризация по названиям/описаниям
        categories['auto_categories'] = self._auto_categorize_items(items_df)
        
        # Анализ ценовых сегментов (с проверкой корректности цен)
        if 'price' in items_df.columns:
            price_stats = self._get_price_stats(items_df)
            if price_stats['is_valid']:
                categories['price_segments'] = self._analyze_price_segments(items_df)
            else:
                print("   ⚠️ Цены выглядят некорректно, пропускаем анализ ценовых сегментов")
                categories['price_segments'] = {'status': 'invalid_prices'}
        
        return categories
    
    def _get_price_stats(self, items_df: pl.DataFrame) -> Dict:
        """Получение статистики цен с проверкой корректности"""
        prices = items_df['price']
        
        min_price = prices.min()
        max_price = prices.max()
        mean_price = prices.mean()
        
        # Проверка на корректность цен
        is_valid = (min_price >= 0 and max_price > 10 and mean_price > 0)
        
        return {
            'min': min_price,
            'max': max_price,
            'mean': mean_price,
            'is_valid': is_valid,
            'issue': 'negative_prices' if min_price < 0 else 'low_prices' if max_price <= 10 else 'ok'
        }
    
    def _analyze_existing_categories(self, items_df: pl.DataFrame) -> Dict:
        """Анализ существующих категорий"""
        # Фильтруем пустые категории
        valid_items = items_df.filter(pl.col('category').is_not_null())
        
        category_stats = (
            valid_items.group_by('category')
            .agg([
                pl.count().alias('item_count'),
                pl.col('price').mean().alias('avg_price'),
                pl.col('price').std().alias('price_std'),
                pl.col('subcategory').unique().alias('subcategories')
            ])
            .sort('item_count', descending=True)
        )
        
        return {
            'stats': category_stats.to_dicts(),
            'total_categories': category_stats.height,
            'top_categories': category_stats.head(10).to_dicts(),
            'total_items_with_category': valid_items.height,
            'items_without_category': items_df.height - valid_items.height
        }
    
    def _analyze_brands(self, items_df: pl.DataFrame) -> Dict:
        """Анализ брендов и их ценовых диапазонов"""
        brand_stats = (
            items_df.group_by('brand_id')
            .agg([
                pl.count().alias('product_count'),
                pl.col('price').mean().alias('avg_price'),
                pl.col('category').unique().alias('categories')
            ])
            .filter(pl.col('product_count') > 5)  # Только значимые бренды
            .sort('avg_price', descending=True)
        )
        
        return {
            'total_brands': brand_stats.height,
            'premium_brands': brand_stats.filter(pl.col('avg_price') > 50000).to_dicts(),
            'midrange_brands': brand_stats.filter(
                (pl.col('avg_price') >= 10000) & (pl.col('avg_price') <= 50000)
            ).to_dicts(),
            'budget_brands': brand_stats.filter(pl.col('avg_price') < 10000).to_dicts()
        }
    
    def _auto_categorize_items(self, items_df: pl.DataFrame) -> Dict:
        """Автоматическая категоризация на основе анализа данных"""
        enhanced_categories = {}
        
        # Анализ по комбинации категория + подкатегория
        if 'category' in items_df.columns and 'subcategory' in items_df.columns:
            category_combo = (
                items_df.filter(pl.col('category').is_not_null() & pl.col('subcategory').is_not_null())
                .group_by(['category', 'subcategory'])
                .agg([
                    pl.count().alias('count'),
                    pl.col('price').mean().alias('avg_price'),
                    pl.col('price').std().alias('price_volatility')
                ])
            )
            
            enhanced_categories['category_combinations'] = category_combo.to_dicts()
        
        # Определение товарных кластеров по цене (только если цены корректны)
        if 'price' in items_df.columns:
            price_stats = self._get_price_stats(items_df)
            if price_stats['is_valid']:
                price_clusters = self._create_price_clusters(items_df)
                enhanced_categories['price_clusters'] = price_clusters
            else:
                enhanced_categories['price_clusters'] = {'status': 'skipped_due_to_invalid_prices'}
        
        return enhanced_categories
    
    def _create_price_clusters(self, items_df: pl.DataFrame) -> List[Dict]:
        """Создание кластеров товаров по цене"""
        # Используем правильный способ получения квантилей
        q25 = items_df['price'].quantile(0.25)
        q75 = items_df['price'].quantile(0.75)
        
        clusters = [
            {
                'segment': 'budget',
                'range': (0, q25),
                'description': 'Бюджетные товары повседневного спроса',
                'item_count': items_df.filter(pl.col('price') <= q25).height
            },
            {
                'segment': 'medium', 
                'range': (q25, q75),
                'description': 'Товары среднего ценового диапазона',
                'item_count': items_df.filter((pl.col('price') > q25) & (pl.col('price') <= q75)).height
            },
            {
                'segment': 'premium',
                'range': (q75, float('inf')),
                'description': 'Премиальные товары и инвестиционные покупки',
                'item_count': items_df.filter(pl.col('price') > q75).height
            }
        ]
        
        return clusters
    
    def _analyze_price_segments(self, items_df: pl.DataFrame) -> Dict:
        """Анализ ценовых сегментов"""
        # Используем абсолютные значения цен для анализа
        price_col = pl.col('price')
        
        segments = {
            'budget': items_df.filter(price_col < 1000),
            'medium': items_df.filter((price_col >= 1000) & (price_col < 10000)),
            'premium': items_df.filter((price_col >= 10000) & (price_col < 50000)),
            'luxury': items_df.filter(price_col >= 50000)
        }
        
        return {
            name: {
                'count': df.height,
                'avg_price': df['price'].mean() if df.height > 0 else 0,
                'price_range': (df['price'].min(), df['price'].max()) if df.height > 0 else (0, 0)
            }
            for name, df in segments.items()
        }