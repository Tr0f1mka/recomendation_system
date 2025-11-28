# src/03_train.py
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
from sklearn.preprocessing import LabelEncoder
import matplotlib.pyplot as plt
import seaborn as sns
import pickle
import os

def train_model():
    print("🤖 ОБУЧАЕМ ML-МОДЕЛЬ...")
    
    # 1. Загружаем фичи
    print("📥 Загружаем фичи пользователей...")
    features_df = pd.read_parquet('user_features.pq')
    
    print(f"📊 Данные для обучения:")
    print(f"- Пользователей: {len(features_df)}")
    print(f"- Фичей: {len(features_df.columns)}")
    print(f"- Распределение целевой переменной:")
    print(features_df['target_product'].value_counts())
    
    # 2. Подготовка данных
    print("\n🔧 Подготавливаем данные...")
    
    # Убираем ненужные колонки
    X = features_df.drop(['user_id', 'target_product'], axis=1, errors='ignore')
    y = features_df['target_product']
    
    # Заполняем пропуски
    X = X.fillna(0)
    
    print(f"📈 Фичи для обучения: {X.columns.tolist()}")
    
    # 3. Кодируем целевую переменную
    label_encoder = LabelEncoder()
    y_encoded = label_encoder.fit_transform(y)
    
    print(f"🎯 Классы: {label_encoder.classes_}")
    
    # 4. Разделяем на train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y_encoded, test_size=0.2, random_state=42, stratify=y_encoded
    )
    
    print(f"📚 Обучающая выборка: {len(X_train)}")
    print(f"🧪 Тестовая выборка: {len(X_test)}")
    
    # 5. Обучаем XGBoost
    print("\n🚀 Обучаем XGBoost модель...")
    
    model = xgb.XGBClassifier(
        n_estimators=100,
        max_depth=6,
        learning_rate=0.1,
        random_state=42,
        eval_metric='mlogloss',
        verbosity=1
    )
    
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=True
    )
    
    # 6. Оценка модели
    print("\n📊 ОЦЕНКА КАЧЕСТВА МОДЕЛИ:")
    
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    
    print(f"🎯 Точность: {accuracy:.2%}")
    print(f"📈 Classification Report:")
    print(classification_report(y_test, y_pred, target_names=label_encoder.classes_))
    
    # 7. Важность признаков
    print("\n🔝 ВАЖНОСТЬ ПРИЗНАКОВ:")
    feature_importance = model.feature_importances_
    importance_df = pd.DataFrame({
        'feature': X.columns,
        'importance': feature_importance
    }).sort_values('importance', ascending=False)
    
    print(importance_df.head(10))
    
    # 8. Визуализация важности признаков
    plt.figure(figsize=(10, 6))
    sns.barplot(data=importance_df.head(10), x='importance', y='feature')
    plt.title('Топ-10 важных признаков')
    plt.tight_layout()
    plt.savefig('feature_importance.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("💾 График сохранен как feature_importance.png")
    
    # 9. Сохраняем модель
    print("\n💾 Сохраняем модель...")
    
    # Создаем папку если нет
    os.makedirs('models', exist_ok=True)
    
    # Сохраняем модель
    model.save_model('models/xgboost_model.json')
    
    # Сохраняем label encoder
    with open('models/label_encoder.pkl', 'wb') as f:
        pickle.dump(label_encoder, f)
    
    # Сохраняем названия фичей
    with open('models/feature_names.pkl', 'wb') as f:
        pickle.dump(X.columns.tolist(), f)
    
    print("✅ Модель и вспомогательные файлы сохранены!")
    
    # 10. Примеры предсказаний
    print("\n👀 ПРИМЕРЫ ПРЕДСКАЗАНИЙ:")
    sample_indices = np.random.choice(len(X_test), 100, replace=False)
    
    for idx in sample_indices:
        user_features = X_test.iloc[idx:idx+1]
        true_label = y_test[idx]
        pred_label = y_pred[idx]
        
        true_product = label_encoder.inverse_transform([true_label])[0]
        pred_product = label_encoder.inverse_transform([pred_label])[0]
        
        print(f"Пользователь {idx}: Истина = {true_product}, Предсказание = {pred_product}")
    
    return model, label_encoder

def analyze_model_performance(model, X_test, y_test, label_encoder):
    """Дополнительный анализ модели"""
    print("\n📈 ДОПОЛНИТЕЛЬНЫЙ АНАЛИЗ:")
    
    # Предсказания вероятностей
    y_pred_proba = model.predict_proba(X_test)
    
    # Средняя уверенность модели
    confidence = np.max(y_pred_proba, axis=1).mean()
    print(f"🤔 Средняя уверенность модели: {confidence:.2%}")
    
    # Распределение уверенности
    confidence_distribution = np.max(y_pred_proba, axis=1)
    print(f"📊 Минимальная уверенность: {confidence_distribution.min():.2%}")
    print(f"📊 Максимальная уверенность: {confidence_distribution.max():.2%}")

if __name__ == "__main__":
    model, label_encoder = train_model()
    
    print(f"\n🎉 МОДЕЛЬ ОБУЧЕНА УСПЕШНО!")
    print(f"📁 Файлы сохранены в папке models/")
    print(f"🎯 Готово к созданию демо-интерфейса!")