# src/05_demo_enhanced.py
import gradio as gr
import pandas as pd
import pickle
import xgboost as xgb
from smart_recommendations import SmartRecommendationEngine

# Загружаем модель и движок рекомендаций
model = xgb.XGBClassifier()
model.load_model('models/xgboost_model.json')

with open('models/label_encoder.pkl', 'rb') as f:
    label_encoder = pickle.load(f)

with open('models/feature_names.pkl', 'rb') as f:
    feature_names = pickle.load(f)

recommendation_engine = SmartRecommendationEngine()

def recommend_for_user(user_id):
    try:
        # Загружаем фичи пользователя
        features_df = pd.read_parquet('user_features.pq')
        user_data = features_df[features_df['user_id'] == user_id]
        
        if len(user_data) == 0:
            return "❌ Пользователь не найден в данных"
        
        # Подготавливаем фичи для ML модели
        user_features = user_data.drop(['user_id', 'target_product'], axis=1, errors='ignore')
        user_features = user_features.fillna(0)
        
        # Получаем рекомендации от умного движка
        recommendations = recommendation_engine.recommend_products(
            user_features.iloc[0].to_dict(), top_n=3
        )
        
        # Форматируем результат
        result = "🎯 **ПЕРСОНАЛИЗИРОВАННЫЕ РЕКОМЕНДАЦИИ**\n\n"
        
        for i, rec in enumerate(recommendations, 1):
            explanation = recommendation_engine.generate_explanation(
                user_features.iloc[0].to_dict(), rec
            )
            
            result += f"{i}. **{rec['name']}**\n"
            result += f"   📊 Совпадение: {rec['match_percentage']}\n"
            result += f"   📝 {rec['description']}\n"
            result += f"   💡 {explanation}\n\n"
        
        # Добавляем статистику пользователя
        result += "---\n"
        result += "📈 **ВАША СТАТИСТИКА:**\n"
        result += f"- Активность: {user_features['market_events'].iloc[0]} событий\n"
        result += f"- Вовлеченность с банком: {user_features['offers_engagement'].iloc[0]}\n"
        result += f"- Уникальных товаров: {user_features['market_unique_items'].iloc[0]}\n"
        
        return result
        
    except Exception as e:
        return f"❌ Ошибка: {str(e)}"

# Создаем интерфейс
demo = gr.Interface(
    fn=recommend_for_user,
    inputs=gr.Textbox(
        label="Введите User ID", 
        placeholder="например: 12345678",
        value="24564205"  # пример из данных
    ),
    outputs=gr.Markdown(label="Рекомендации"),
    title="🏦 Умная рекомендательная система ПСБ",
    description="Введите ID пользователя для получения персонализированных рекомендаций банковских продуктов",
    examples=[
        ["24564205"], 
        ["46708173"],
        ["34337422"]
    ]
)

if __name__ == "__main__":
    demo.launch(
        share=True,
        server_name="0.0.0.0", 
        server_port=7860
    )