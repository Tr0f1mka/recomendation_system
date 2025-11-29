import polars as pl
import json
from typing import Dict, List, Optional
import logging

class FreeLLMProcessor:
    """Бесплатные LLM решения с реальными категориями из данных"""
    
    def __init__(self, discovered_categories: Dict):
        self.cache = {}
        self.enabled = True
        self.discovered_categories = discovered_categories
        self.available_categories = self._extract_categories_from_data()
        
    def _extract_categories_from_data(self) -> List[str]:
        """Извлечение реальных категорий из анализа данных"""
        categories = set()
        
        # Берем категории из category_discovery
        if 'existing_categories' in self.discovered_categories:
            stats = self.discovered_categories['existing_categories'].get('stats', [])
            for item in stats:
                if 'category' in item and item['category']:
                    categories.add(item['category'])
        
        # Берем категории из auto_categories
        if 'auto_categories' in self.discovered_categories:
            auto_cats = self.discovered_categories['auto_categories']
            if 'category_combinations' in auto_cats:
                for combo in auto_cats['category_combinations']:
                    if 'category' in combo and combo['category']:
                        categories.add(combo['category'])
        
        # Если категорий нет, используем умные fallback
        if not categories:
            categories = self._get_smart_fallback_categories()
        
        return list(categories)
    
    def _get_smart_fallback_categories(self) -> List[str]:
        """Умные fallback категории на основе доменной логики"""
        return [
            "электроника",
            "бытовая техника", 
            "мебель",
            "одежда",
            "обувь",
            "аксессуары",
            "косметика",
            "продукты питания",
            "напитки",
            "хозтовары",
            "строительные материалы",
            "автомобильные товары",
            "книги",
            "спортивные товары",
            "детские товары",
            "ювелирные изделия",
            "услуги"
        ]
    
    def analyze_product(self, product_name: str, items_df: pl.DataFrame = None) -> Dict:
        """Анализ продукта с использованием реальных категорий из данных"""
        cache_key = product_name
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        # Пытаемся найти товар в данных для точной категоризации
        exact_match = self._find_exact_product_match(product_name, items_df)
        if exact_match:
            result = self._analyze_from_data(exact_match)
        else:
            # Семантический анализ на основе реальных категорий
            result = self._semantic_analysis(product_name)
        
        self.cache[cache_key] = result
        return result
    
    def _find_exact_product_match(self, product_name: str, items_df: pl.DataFrame) -> Optional[Dict]:
        """Поиск точного совпадения товара в данных"""
        if items_df is None or items_df.height == 0:
            return None
            
        try:
            # Ищем похожие названия товаров
            name_lower = product_name.lower()
            
            # Простой поиск по подстроке
            matches = items_df.filter(
                pl.col('item_name').str.to_lowercase().str.contains(name_lower) |
                pl.col('name').str.to_lowercase().str.contains(name_lower)
            )
            
            if matches.height > 0:
                # Берем первый найденный товар
                match_row = matches.row(0, named=True)
                return {
                    'name': match_row.get('item_name') or match_row.get('name', ''),
                    'category': match_row.get('category'),
                    'subcategory': match_row.get('subcategory'),
                    'price': match_row.get('price', 0),
                    'brand': match_row.get('brand_id')
                }
                
        except Exception as e:
            logging.debug(f"Поиск товара не удался: {e}")
            
        return None
    
    def _analyze_from_data(self, product_data: Dict) -> Dict:
        """Анализ на основе реальных данных о товаре"""
        category = product_data.get('category', 'unknown')
        price = product_data.get('price', 0)
        
        return {
            "primary_category": category,
            "price_segment": self._calculate_price_segment(price),
            "purchase_type": self._infer_purchase_type_from_category(category),
            "financial_products": self._infer_financial_products_from_data(category, price),
            "spending_impact": self._calculate_spending_impact(price),
            "data_based": True,
            "confidence": 0.9,
            "reasoning": f"Определено на основе данных: {category}"
        }
    
    def _semantic_analysis(self, product_name: str) -> Dict:
        """Семантический анализ когда точных данных нет"""
        # Используем реальные категории из данных для анализа
        best_category = self._find_best_category_match(product_name)
        estimated_price = self._estimate_price_from_category(best_category)
        
        return {
            "primary_category": best_category,
            "price_segment": self._calculate_price_segment(estimated_price),
            "purchase_type": self._infer_purchase_type_from_category(best_category),
            "financial_products": self._infer_financial_products_from_data(best_category, estimated_price),
            "spending_impact": self._calculate_spending_impact(estimated_price),
            "data_based": False,
            "confidence": 0.6,
            "reasoning": f"Семантический анализ: {best_category}"
        }
    
    def _find_best_category_match(self, product_name: str) -> str:
        """Нахождение лучшего соответствия категории"""
        if not self.available_categories:
            return "other"
        
        name_lower = product_name.lower()
        
        # Простой семантический матчинг
        for category in self.available_categories:
            category_lower = category.lower()
            
            # Проверяем семантическое сходство
            if self._check_semantic_similarity(name_lower, category_lower):
                return category
        
        # Если не нашли, возвращаем самую частую категорию или "other"
        return self.available_categories[0] if self.available_categories else "other"
    
    def _check_semantic_similarity(self, product_name: str, category: str) -> bool:
        """Проверка семантического сходства"""
        # Простая эвристика для демо - в реальности можно использовать embedding
        product_words = set(product_name.split())
        category_words = set(category.split())
        
        # Есть ли общие слова
        common_words = product_words.intersection(category_words)
        return len(common_words) > 0
    
    def _calculate_price_segment(self, price: float) -> str:
        """Расчет ценового сегмента на основе данных"""
        if price <= 0:
            return "medium"
        elif price < 1000:
            return "budget"
        elif price < 10000:
            return "medium"
        elif price < 50000:
            return "premium"
        else:
            return "luxury"
    
    def _estimate_price_from_category(self, category: str) -> float:
        """Оценка цены на основе категории"""
        # В реальной системе здесь должна быть статистика по категориям
        price_ranges = {
            "электроника": 15000,
            "бытовая техника": 8000,
            "мебель": 12000,
            "одежда": 3000,
            "продукты питания": 500,
            "услуги": 2000
        }
        return price_ranges.get(category, 5000)
    
    def _infer_purchase_type_from_category(self, category: str) -> str:
        """Определение типа покупки на основе категории"""
        frequent_categories = ["продукты питания", "напитки", "хозтовары"]
        investment_categories = ["электроника", "мебель", "бытовая техника", "автомобильные товары"]
        
        if category in frequent_categories:
            return "daily"
        elif category in investment_categories:
            return "investment"
        else:
            return "rare"
    
    def _infer_financial_products_from_data(self, category: str, price: float) -> List[str]:
        """Определение финансовых продуктов на основе данных"""
        products = ["credit_card"]
        
        if price > 10000:
            products.extend(["installment", "insurance"])
        
        if category in ["электроника", "бытовая техника", "автомобильные товары"]:
            products.append("insurance")
            
        if category in ["мебель", "услуги"]:
            products.append("savings")
            
        if price > 50000:
            products.append("investment")
            
        return list(set(products))
    
    def _calculate_spending_impact(self, price: float) -> str:
        """Расчет влияния на траты"""
        if price <= 0:
            return "medium"
        elif price < 5000:
            return "low"
        elif price < 20000:
            return "medium"
        else:
            return "high"
    
    def analyze_user_behavior(self, user_purchases: List[Dict]) -> Dict:
        """Анализ поведения пользователя на основе реальных покупок"""
        if not user_purchases:
            return self._get_default_user_profile()
        
        # Анализируем реальные покупки
        total_spent = sum(p.get('price', 0) for p in user_purchases)
        categories = [p.get('category', 'unknown') for p in user_purchases if p.get('category')]
        unique_categories = set(categories)
        
        # Определяем профиль на основе данных
        avg_purchase = total_spent / len(user_purchases) if user_purchases else 0
        
        return {
            "financial_profile": self._determine_financial_profile(total_spent, avg_purchase),
            "main_interests": list(unique_categories)[:3],
            "spending_habits": self._determine_spending_habits(avg_purchase, len(unique_categories)),
            "recommended_focus": self._determine_recommended_focus(total_spent, unique_categories),
            "data_based": True,
            "total_purchases": len(user_purchases),
            "total_spent": total_spent
        }
    
    def _determine_financial_profile(self, total_spent: float, avg_purchase: float) -> str:
        """Определение финансового профиля"""
        if total_spent > 100000 or avg_purchase > 20000:
            return "премиальный"
        elif total_spent > 50000 or avg_purchase > 10000:
            return "выше среднего"
        elif total_spent > 20000 or avg_purchase > 5000:
            return "средний"
        else:
            return "бюджетный"
    
    def _determine_spending_habits(self, avg_purchase: float, unique_categories: int) -> str:
        """Определение привычек трат"""
        if avg_purchase > 15000 and unique_categories > 5:
            return "разнообразный"
        elif avg_purchase > 10000:
            return "щедрый"
        elif avg_purchase < 3000:
            return "экономный"
        else:
            return "умеренный"
    
    def _determine_recommended_focus(self, total_spent: float, categories: set) -> List[str]:
        """Определение фокуса рекомендаций"""
        focus = ["credit_card"]
        
        if total_spent > 50000:
            focus.extend(["investment", "premium_cards"])
            
        if any(cat in categories for cat in ["электроника", "бытовая техника"]):
            focus.append("insurance")
            
        if total_spent > 100000:
            focus.append("savings")
            
        return focus
    
    def _get_default_user_profile(self) -> Dict:
        return {
            "financial_profile": "средний",
            "main_interests": ["general"],
            "spending_habits": "умеренный",
            "recommended_focus": ["credit_card"],
            "data_based": False
        }