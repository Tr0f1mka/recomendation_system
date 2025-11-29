import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
import joblib
import os
from typing import Dict, List

class MLEnhancer:
    """ML модель для улучшения рекомендаций"""
    
    def __init__(self):
        self.model = RandomForestRegressor(
            n_estimators=50, 
            random_state=42, 
            max_depth=10,
            min_samples_split=5
        )
        self.scaler = StandardScaler()
        self.is_trained = False
        
    def train(self, training_data: List[Dict]):
        """Обучение модели на исторических данных"""
        if not training_data:
            print("⚠️ Нет данных для обучения ML модели")
            return False
            
        print(f"🧠 Обучение ML модели на {len(training_data)} примерах...")
        
        try:
            X = []
            y = []
            
            for record in training_data:
                features = self._extract_features(record)
                X.append(features)
                y.append(record.get('conversion_rate', 0.5))
            
            X_array = np.array(X)
            
            # Масштабируем фичи
            X_scaled = self.scaler.fit_transform(X_array)
            
            # Обучаем модель
            self.model.fit(X_scaled, y)
            self.is_trained = True
            
            # Сохраняем модель
            os.makedirs("models", exist_ok=True)
            joblib.dump({
                'model': self.model,
                'scaler': self.scaler
            }, 'models/ml_enhancer.pkl')
            
            score = self.model.score(X_scaled, y)
            print(f"✅ ML модель обучена! R² score: {score:.3f}")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка обучения ML модели: {e}")
            return False
    
    def predict(self, user_profile: Dict, product: Dict) -> float:
        """Предсказание score с помощью ML модели"""
        if not self.is_trained:
            return 0.5
            
        try:
            features = self._extract_features({
                'user_profile': user_profile,
                'product': product
            })
            features_scaled = self.scaler.transform([features])
            prediction = self.model.predict(features_scaled)[0]
            return float(np.clip(prediction, 0, 1))
        except Exception as e:
            print(f"⚠️ Ошибка ML предсказания: {e}")
            return 0.5
    
    def optimize(self, base_score: float, user_profile: Dict, product: Dict) -> float:
        """Оптимизация рекомендации с помощью ML"""
        ml_score = self.predict(user_profile, product)
        optimized_score = base_score * 0.6 + ml_score * 0.4
        return round(optimized_score, 3)
    
    def _extract_features(self, record: Dict) -> List[float]:
        """Извлечение признаков из данных"""
        user_profile = record['user_profile']
        product = record['product']
        
        features = [
            # Фичи пользователя
            user_profile.get('total_spent', 0),
            user_profile.get('avg_transaction_value', 0),
            self._map_activity_level(user_profile.get('interaction_frequency', 'unknown')),
            user_profile.get('category_diversity', 0),
            
            # Фичи продукта
            product.get('business_value', 0.5),
            
            # Взаимодействия
            self._calculate_spending_match(user_profile, product)
        ]
        
        return features
    def load_model(self, model_path: str = "models/ml_enhancer.pkl"):
        """Загрузка предварительно обученной модели"""
        try:
            if os.path.exists(model_path):
                model_data = joblib.load(model_path)
                self.model = model_data['model']
                self.scaler = model_data['scaler']
                self.is_trained = True
                print("✅ ML модель загружена из файла")
            else:
                print("⚠️ Файл модели не найден, модель не обучена")
        except Exception as e:
            print(f"❌ Ошибка загрузки модели: {e}")

    def train_model(self, training_data: List[Dict]):
        """Алиас для train для совместимости"""
        return self.train(training_data)
    
    def _map_activity_level(self, activity: str) -> float:
        """Маппинг активности в числовое значение"""
        activity_map = {
            'very_low': 0.1, 'low': 0.3, 'medium': 0.5,
            'high': 0.7, 'very_high': 0.9, 'unknown': 0.5
        }
        return activity_map.get(activity, 0.5)
    
    def _calculate_spending_match(self, user_profile: Dict, product: Dict) -> float:
        """Совпадение уровня трат пользователя и продукта"""
        user_spending = user_profile.get('total_spent', 0)
        product_value = product.get('business_value', 0.5)
        
        # Нормализованное совпадение
        if user_spending > 50000 and product_value > 0.7:
            return 0.9
        elif user_spending > 20000 and product_value > 0.5:
            return 0.7
        elif user_spending > 5000:
            return 0.5
        else:
            return 0.3