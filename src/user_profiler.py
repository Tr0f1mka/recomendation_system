import polars as pl
import numpy as np
from typing import Dict, List
from datetime import datetime, timedelta

class UserProfiler:
    def __init__(self, discovered_categories: Dict):
        self.categories = discovered_categories
        self.user_profiles = {}
    
    def create_user_profiles(self, users_df: pl.DataFrame, events_df: pl.DataFrame, items_df: pl.DataFrame) -> pl.DataFrame:
        """Создание профилей пользователей на основе их поведения"""
        print("👤 Создание пользовательских профилей...")
        
        if events_df.height == 0:
            print("❌ Нет событий для анализа")
            return pl.DataFrame()
        
        # Объединяем события с информацией о товарах
        enriched_events = self._enrich_events_with_item_data(events_df, items_df)
        
        if enriched_events.height == 0:
            print("❌ Не удалось обогатить события данными о товарах")
            return pl.DataFrame()
        
        # Берем пользователей из событий
        users_from_events = enriched_events['user_id'].unique()
        print(f"   Пользователей для анализа: {users_from_events.len()}")  # Исправлено: .len() вместо .length()
        
        # Создаем профили для всех пользователей из событий (ограничим для скорости)
        sample_size = min(users_from_events.len(), 1000)
        sample_users = users_from_events.head(sample_size)
        
        user_profiles = []
        processed = 0
        success_count = 0
        
        for user_id in sample_users:
            try:
                profile = self._create_single_user_profile(user_id, enriched_events)
                if profile:
                    user_profiles.append(profile)
                    success_count += 1
                
                processed += 1
                
                # Прогресс
                if processed % 50 == 0:
                    print(f"   Обработано {processed}/{sample_size} пользователей, успешно: {success_count}")
                    
            except Exception as e:
                print(f"      ❌ Ошибка создания профиля для {user_id}: {e}")
                continue
    
        print(f"   ✅ Успешно создано профилей: {len(user_profiles)}")
        return pl.DataFrame(user_profiles) if user_profiles else pl.DataFrame()
    
    def _enrich_events_with_item_data(self, events_df: pl.DataFrame, items_df: pl.DataFrame) -> pl.DataFrame:
        """Обогащение событий данными о товарах"""
        if events_df.height == 0 or items_df.height == 0:
            print("⚠️ Нет событий или товаров для обогащения")
            return events_df
            
        print(f"   Обогащение {events_df.height} событий данными о {items_df.height} товарах...")
        
        try:
            # Используем price_fixed если есть, иначе создаем
            if 'price_fixed' not in items_df.columns:
                items_df = items_df.with_columns([
                    pl.when(pl.col('category').is_not_null())
                     .then((pl.col('category').hash() % 9000) + 1000)
                     .otherwise(2000)
                     .alias('price_fixed')
                ])
            
            enriched = events_df.join(
                items_df.select(['item_id', 'category', 'subcategory', 'price_fixed']),
                on='item_id',
                how='left'
            )
            print(f"   После обогащения: {enriched.height} событий")
            return enriched
        except Exception as e:
            print(f"❌ Ошибка обогащения событий: {e}")
            return events_df
    
    def _create_single_user_profile(self, user_id: str, enriched_events: pl.DataFrame) -> Dict:
        """Создание профиля для одного пользователя"""
        user_events = enriched_events.filter(pl.col('user_id') == user_id)
        
        if user_events.height == 0:
            return None
        
        print(f"      Обработка пользователя {user_id}: {user_events.height} событий")
        
        try:
            # Базовые метрики с обработкой ошибок
            spending_metrics = self._calculate_spending_metrics(user_events)
            behavioral_metrics = self._calculate_behavioral_metrics(user_events)
            category_affinity = self._calculate_category_affinity(user_events)
            temporal_patterns = self._analyze_temporal_patterns(user_events)
            
            profile = {
                'user_id': user_id,
                **spending_metrics,
                **behavioral_metrics,
                'category_affinity': category_affinity,
                **temporal_patterns,
                'total_interactions': user_events.height,
                'profile_completeness': self._calculate_profile_completeness(user_events)
            }
            
            return profile
            
        except Exception as e:
            print(f"      ❌ Ошибка создания профиля для {user_id}: {e}")
            return None
    
    def _calculate_spending_metrics(self, user_events: pl.DataFrame) -> Dict:
        """Расчет финансовых метрик - гарантируем числа"""
        price_col = 'price_fixed' if 'price_fixed' in user_events.columns else 'price'
        
        if price_col not in user_events.columns:
            return {
                'total_spent': 0,
                'avg_transaction_value': 0,
                'spending_level': 'unknown',
                'max_transaction': 0,
                'spending_consistency': 0
            }
        
        try:
            price_data = user_events[price_col]
            total_spent = price_data.sum() or 0
            avg_value = price_data.mean() or 0
            max_value = price_data.max() or 0
            
            # Уровень трат
            if total_spent > 50000:
                spending_level = "very_high"
            elif total_spent > 20000:
                spending_level = "high" 
            elif total_spent > 5000:
                spending_level = "medium"
            elif total_spent > 1000:
                spending_level = "low"
            else:
                spending_level = "very_low"
            
            # Стабильность трат с обработкой деления на ноль
            std_dev = price_data.std() or 0
            spending_consistency = std_dev / avg_value if avg_value > 0 else 0
            
            return {
                'total_spent': float(total_spent),
                'avg_transaction_value': float(avg_value),
                'spending_level': spending_level,
                'max_transaction': float(max_value),
                'spending_consistency': float(spending_consistency)
            }
        except Exception as e:
            print(f"      ⚠️ Ошибка расчета финансовых метрик: {e}")
            return {
                'total_spent': 0,
                'avg_transaction_value': 0,
                'spending_level': 'unknown',
                'max_transaction': 0,
                'spending_consistency': 0
            }
        
    def _calculate_behavioral_metrics(self, user_events: pl.DataFrame) -> Dict:
        """Расчет поведенческих метрик"""
        total_events = user_events.height
        
        # Частота взаимодействий
        if 'timestamp' in user_events.columns:
            try:
                time_range = user_events['timestamp'].max() - user_events['timestamp'].min()
                days = time_range.days if hasattr(time_range, 'days') and time_range.days > 0 else 1
                events_per_day = total_events / days
                
                if events_per_day > 10:
                    frequency = "very_high"
                elif events_per_day > 5:
                    frequency = "high"
                elif events_per_day > 2:
                    frequency = "medium" 
                else:
                    frequency = "low"
            except:
                frequency = "unknown"
        else:
            frequency = "unknown"
        
        # Разнообразие взаимодействий
        unique_categories = user_events['category'].n_unique() if 'category' in user_events.columns else 0  # Исправлено: n_unique() вместо unique().length()
        diversity_score = unique_categories / total_events if total_events > 0 else 0
        
        return {
            'interaction_frequency': frequency,
            'category_diversity': float(diversity_score),
            'unique_categories_count': unique_categories,
            'preference_stability': float(self._calculate_preference_stability(user_events))
        }
    
    def _calculate_category_affinity(self, user_events: pl.DataFrame) -> Dict:
        """Расчет аффинити к категориям - гарантируем возврат словаря"""
        if 'category' not in user_events.columns:
            return {}  # ФИКС: всегда возвращаем словарь
        
        try:
            # Фильтруем пустые категории
            valid_events = user_events.filter(
                pl.col('category').is_not_null() & 
                (pl.col('category') != '') &
                (pl.col('category') != 'null')
            )
            
            if valid_events.height == 0:
                return {}  # ФИКС: возвращаем пустой словарь вместо None
                
            category_stats = (
                valid_events.group_by('category')
                .agg([
                    pl.count().alias('count'),
                    pl.col('price_fixed').sum().alias('total_spent')
                ])
                .sort('count', descending=True)
            )
            
            total_interactions = category_stats['count'].sum()
            
            affinity = {}
            for row in category_stats.iter_rows(named=True):
                category = row['category']
                if category and category != 'null':  # ФИКС: проверяем валидность категории
                    interaction_ratio = row['count'] / total_interactions if total_interactions > 0 else 0
                    
                    # ФИКС: безопасный расчет spending_ratio
                    total_user_spent = valid_events['price_fixed'].sum()
                    spending_ratio = row['total_spent'] / total_user_spent if total_user_spent > 0 else 0
                    
                    # Комбинированный score
                    affinity_score = (interaction_ratio * 0.6 + spending_ratio * 0.4)
                    affinity[category] = round(float(affinity_score), 3)
            
            return dict(sorted(affinity.items(), key=lambda x: x[1], reverse=True)[:10])
            
        except Exception as e:
            print(f"      ⚠️ Ошибка расчета category affinity: {e}")
            return {}  # ФИКС: всегда возвращаем словарь даже при ошибке
    
    def _calculate_preference_stability(self, user_events: pl.DataFrame) -> float:
        """Расчет стабильности предпочтений"""
        if 'category' not in user_events.columns or user_events.height < 10:
            return 0.5
        
        try:
            # Разделяем события на две половины и сравниваем предпочтения
            half_point = user_events.height // 2
            first_half = user_events.head(half_point)
            second_half = user_events.tail(user_events.height - half_point)
            
            # ИСПРАВЛЕНИЕ: правильно получаем уникальные категории
            first_categories = set(first_half['category'].drop_nulls().unique().to_list())
            second_categories = set(second_half['category'].drop_nulls().unique().to_list())
            
            overlap = len(first_categories.intersection(second_categories))
            total = len(first_categories.union(second_categories))
            
            return overlap / total if total > 0 else 0.5
        except Exception as e:
            print(f"      ⚠️ Ошибка расчета стабильности предпочтений: {e}")
            return 0.5

    def _analyze_temporal_patterns(self, user_events: pl.DataFrame) -> Dict:
        """Анализ временных паттернов - гарантируем возврат чисел"""
        if 'timestamp' not in user_events.columns:
            return {
                'temporal_consistency': 0.5,
                'activity_duration_days': 0  # ФИКС: всегда число
            }
        
        try:
            dates = user_events['timestamp'].sort()
            if dates.len() < 2:
                return {
                    'temporal_consistency': 0.5,
                    'activity_duration_days': 0  # ФИКС: всегда число
                }
            
            # Расчет длительности активности
            try:
                duration = dates.max() - dates.min()
                if hasattr(duration, 'days'):
                    duration_days = duration.days
                else:
                    # Если это timedelta в секундах
                    duration_days = duration.total_seconds() / 86400
            except:
                duration_days = 0  # ФИКС: дефолт при ошибке
                
            # Расчет консистентности
            try:
                if hasattr(dates[0], 'timestamp'):
                    time_diffs = np.diff([d.timestamp() for d in dates.to_list()])
                else:
                    time_diffs = np.diff([d.total_seconds() for d in dates.to_list()])
                
                consistency = 1.0 / (1.0 + np.std(time_diffs) / 86400) if len(time_diffs) > 0 else 0.5
            except:
                consistency = 0.5  # ФИКС: дефолт при ошибке
            
            return {
                'temporal_consistency': min(float(consistency), 1.0),
                'activity_duration_days': int(duration_days)  # ФИКС: гарантированно число
            }
        except Exception as e:
            print(f"      ⚠️ Ошибка анализа временных паттернов: {e}")
            return {
                'temporal_consistency': 0.5,
                'activity_duration_days': 0  # ФИКС: всегда число
            }
        
    def _calculate_profile_completeness(self, user_events: pl.DataFrame) -> float:
        """Расчет полноты профиля"""
        completeness_factors = []
        
        if user_events.height >= 5:
            completeness_factors.append(0.3)
        
        if 'price_fixed' in user_events.columns and user_events['price_fixed'].sum() > 0:
            completeness_factors.append(0.3)
            
        if 'category' in user_events.columns and user_events['category'].n_unique() >= 2:  # Исправлено: n_unique()
            completeness_factors.append(0.2)
            
        if 'timestamp' in user_events.columns and user_events['timestamp'].n_unique() >= 3:  # Исправлено: n_unique()
            completeness_factors.append(0.2)
        
        return float(sum(completeness_factors))