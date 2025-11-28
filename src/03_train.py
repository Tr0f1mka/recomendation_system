# src/03_train_enhanced.py
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix
from sklearn.preprocessing import LabelEncoder
from sklearn.utils import class_weight
import matplotlib.pyplot as plt
import seaborn as sns
import pickle
import os

def train_enhanced_model():
    print("🤖 ОБУЧАЕМ ML С 10 КАТЕГОРИЯМИ...")
    
    # 1. Загружаем расширенные фичи
    print("📥 Загружаем расширенные фичи...")
    features_df = pd.read_parquet('user_features_enhanced.pq')
    
    print(f"📊 Данные для обучения:")
    print(f"- Пользователей: {len(features_df)}")
    print(f"- Фичей: {len(features_df.columns)}")
    print(f"- Распределение категорий:")
    print(features_df['target_product'].value_counts())
    
    # 2. Подготовка данных
    print("\n🔧 Подготавливаем данные...")
    X = features_df.drop(['user_id', 'target_product'], axis=1, errors='ignore')
    y = features_df['target_product']
    
    # Заполняем пропуски
    X = X.fillna(0)
    
    print(f"📈 Фичи для обучения: {len(X.columns)}")
    
    # 3. Кодируем целевую переменную (10 классов!)
    label_encoder = LabelEncoder()
    y_encoded = label_encoder.fit_transform(y)
    
    print(f"🎯 Классы ({len(label_encoder.classes_)}): {label_encoder.classes_}")
    
    # 4. Балансируем классы (важно для многоклассовой классификации)
    class_weights = class_weight.compute_sample_weight(
        'balanced',
        y_encoded
    )
    
    # 5. Разделяем на train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y_encoded, test_size=0.2, random_state=42, stratify=y_encoded
    )
    
    print(f"📚 Обучающая выборка: {len(X_train)}")
    print(f"🧪 Тестовая выборка: {len(X_test)}")
    
    # 6. Обучаем XGBoost с настройками для многоклассовой классификации
    print("\n🚀 Обучаем XGBoost для 10 категорий...")
    
    model = xgb.XGBClassifier(
        n_estimators=150,           # Больше деревьев для сложной классификации
        max_depth=8,                # Глубже для сложных паттернов
        learning_rate=0.1,
        random_state=42,
        eval_metric='mlogloss',     # Метрика для многоклассовой классификации
        verbosity=1,
        scale_pos_weight=1,
        subsample=0.8,
        colsample_bytree=0.8
    )
    
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=10,
        sample_weight=class_weights[:len(X_train)]  # Веса для балансировки
    )
    
    # 7. Оценка модели
    print("\n📊 ОЦЕНКА КАЧЕСТВА МОДЕЛИ (10 КАТЕГОРИЙ):")
    
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    
    print(f"🎯 Точность: {accuracy:.2%}")
    print(f"📈 Classification Report:")
    print(classification_report(y_test, y_pred, target_names=label_encoder.classes_))
    
    # 8. Матрица ошибок
    cm = confusion_matrix(y_test, y_pred)
    plt.figure(figsize=(12, 10))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', 
                xticklabels=label_encoder.classes_,
                yticklabels=label_encoder.classes_)
    plt.title('Матрица ошибок - 10 категорий')
    plt.xlabel('Предсказание')
    plt.ylabel('Истина')
    plt.xticks(rotation=45)
    plt.yticks(rotation=0)
    plt.tight_layout()
    plt.savefig('confusion_matrix_10_classes.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("💾 Матрица ошибок сохранена как confusion_matrix_10_classes.png")
    
    # 9. Важность признаков
    print("\n🔝 ВАЖНОСТЬ ПРИЗНАКОВ:")
    feature_importance = model.feature_importances_
    importance_df = pd.DataFrame({
        'feature': X.columns,
        'importance': feature_importance
    }).sort_values('importance', ascending=False)
    
    print(importance_df.head(15))
    
    # 10. Сохраняем модель
    print("\n💾 Сохраняем модель...")
    os.makedirs('models', exist_ok=True)
    
    model.save_model('models/xgboost_model_enhanced.json')
    
    with open('models/label_encoder_enhanced.pkl', 'wb') as f:
        pickle.dump(label_encoder, f)
    
    with open('models/feature_names_enhanced.pkl', 'wb') as f:
        pickle.dump(X.columns.tolist(), f)
    
    # 11. Примеры предсказаний
    print("\n👀 ПРИМЕРЫ ПРЕДСКАЗАНИЙ (10 КАТЕГОРИЙ):")
    sample_indices = np.random.choice(len(X_test), 8, replace=False)
    
    for idx in sample_indices:
        user_features = X_test.iloc[idx:idx+1]
        true_label = y_test[idx]
        pred_label = y_pred[idx]
        
        true_product = label_encoder.inverse_transform([true_label])[0]
        pred_product = label_encoder.inverse_transform([pred_label])[0]
        
        status = "✅" if true_product == pred_product else "❌"
        print(f"{status} Пользователь {idx}: Истина = {true_product:20} Предсказание = {pred_product}")
    
    print(f"\n🎉 МОДЕЛЬ С 10 КАТЕГОРИЯМИ ОБУЧЕНА!")
    print(f"📊 Реальная точность: {accuracy:.2%}")

if __name__ == "__main__":
    train_enhanced_model()