# src/bank_products_full.py
BANK_PRODUCTS_FULL = {
    # 💰 ВКЛАДЫ И СБЕРЕЖЕНИЯ
    'deposit_savings': {
        'name': 'Вклад «ПСБ.Накопительный»',
        'category': 'savings',
        'description': 'Ставка до 9,5% годовых, пополнение и частичное снятие без потери процентов',
        'target_users': ['stable_savers', 'flexible_savings'],
        'requirements': {'min_activity': 10, 'max_activity': 60}
    },
    
    'deposit_profitable': {
        'name': 'Вклад «ПСБ.Выгодный»',
        'category': 'savings', 
        'description': 'Фиксированная ставка до 10,2% годовых, без пополнения и снятия',
        'target_users': ['long_term_savers', 'high_yield_seekers'],
        'requirements': {'min_activity': 20, 'consistent_behavior': True}
    },
    
    'deposit_pension': {
        'name': 'Вклад «ПСБ.Пенсионный»',
        'category': 'savings',
        'description': 'Повышенная ставка для пенсионеров, пополнение разрешено',
        'target_users': ['low_activity', 'stable_behavior'],
        'requirements': {'max_activity': 30, 'low_engagement': True}
    },
    
    'savings_free': {
        'name': 'Сберегательный счёт «ПСБ.Свободный»',
        'category': 'savings',
        'description': 'Ставка до 8% годовых, полная свобода пополнения и снятия',
        'target_users': ['flexible_savers', 'moderate_activity'],
        'requirements': {'min_activity': 15, 'max_activity': 80}
    },
    
    # 💳 КАРТЫ
    'premium_card': {
        'name': 'Премиальная дебетовая карта «ПСБ.Карта Premium»',
        'category': 'cards',
        'description': 'Кэшбэк до 10%, процент на остаток до 7% годовых, персональный менеджер',
        'target_users': ['high_activity', 'premium_brands', 'frequent_shoppers'],
        'requirements': {'min_activity': 100, 'high_engagement': True}
    },
    
    'credit_card_180': {
        'name': 'Кредитная карта «180 дней без %»',
        'category': 'cards',
        'description': 'Льготный период 180 дней, кэшбэк до 10% на маркетплейсах',
        'target_users': ['active_shoppers', 'tech_lovers', 'frequent_purchases'],
        'requirements': {'market_engagement': 0.1, 'tech_interest': 0.4}
    },
    
    'salary_card_pro': {
        'name': 'ПСБ.Зарплата PRO',
        'category': 'cards',
        'description': 'Бесплатное обслуживание, кэшбэк 1% на всё, до 30% у партнёров',
        'target_users': ['regular_activity', 'stable_behavior', 'consistent_spending'],
        'requirements': {'min_activity': 30, 'consistent_activity': True}
    },
    
    'sports_card': {
        'name': 'Карта «Только вперёд»',
        'category': 'cards',
        'description': 'Кэшбэк 7% в категориях «Спорт и активный отдых», «Аптеки и медицина»',
        'target_users': ['sports_interest', 'healthy_lifestyle', 'active_people'],
        'requirements': {'sports_interest': 0.3, 'medium_activity': True}
    },
    
    'pension_card': {
        'name': 'ПСБ.Пенсионная карта',
        'category': 'cards', 
        'description': 'Кэшбэк до 5% при зачислении пенсии, до 20% у партнёров',
        'target_users': ['low_activity', 'stable_savers', 'senior_users'],
        'requirements': {'max_activity': 25, 'low_engagement': True}
    },
    
    # 🏠 КРЕДИТЫ И ИПОТЕКА
    'consumer_loan': {
        'name': 'Потребительский кредит наличными',
        'category': 'loans',
        'description': 'Сумма до 5 млн ₽, срок до 7 лет, ставка от 6,9% годовых',
        'target_users': ['active_spenders', 'big_purchases', 'high_engagement'],
        'requirements': {'min_activity': 60, 'offers_engagement': 5}
    },
    
    'mortgage': {
        'name': 'Ипотека на новостройку',
        'category': 'loans',
        'description': 'Ставка от 19,49%, сумма до 50 млн ₽, первый взнос от 20%',
        'target_users': ['home_interest', 'family_planning', 'stable_income'],
        'requirements': {'home_interest': 0.5, 'min_activity': 40}
    },
    
    'family_mortgage': {
        'name': 'Семейная ипотека',
        'category': 'loans',
        'description': 'Ставка от 5,9%, до 12 млн ₽, можно использовать маткапитал',
        'target_users': ['family_users', 'home_interest', 'stable_behavior'],
        'requirements': {'home_interest': 0.6, 'consistent_activity': True}
    },
    
    'refinancing': {
        'name': 'Рефинансирование кредитов',
        'category': 'loans',
        'description': 'Объединение до 5 кредитов, ставка от 5,9% годовых',
        'target_users': ['multiple_credits', 'debt_optimization', 'financial_management'],
        'requirements': {'min_activity': 50, 'high_engagement': True}
    },
    
    # 📈 ИНВЕСТИЦИИ
    'investment_stocks': {
        'name': 'ОПИФ «ПРОМСВЯЗЬ — Акции»',
        'category': 'investments',
        'description': 'Фонд акций российских эмитентов, ориентирован на рост стоимости',
        'target_users': ['high_risk_tolerance', 'tech_interest', 'financial_savvy'],
        'requirements': {'tech_interest': 0.6, 'min_activity': 80}
    },
    
    'investment_bonds': {
        'name': 'ОПИФ «ПРОМСВЯЗЬ — Облигации»',
        'category': 'investments',
        'description': 'Консервативный фонд, ориентирован на стабильный доход',
        'target_users': ['low_risk_tolerance', 'stable_savers', 'moderate_activity'],
        'requirements': {'min_activity': 40, 'consistent_behavior': True}
    },
    
    'investment_dividend': {
        'name': 'ОПИФ «Дивидендные акции»',
        'category': 'investments', 
        'description': 'Фонд, нацеленный на получение стабильного дохода за счёт дивидендов',
        'target_users': ['income_seekers', 'stable_investors', 'medium_activity'],
        'requirements': {'min_activity': 60, 'engagement_ratio': 0.08}
    },
    
    # 🛡️ СТРАХОВАНИЕ
    'insurance_life': {
        'name': 'Страхование жизни и здоровья',
        'category': 'insurance',
        'description': 'Добровольное страхование, снижает ставку по кредиту на 1–2%',
        'target_users': ['responsible_users', 'family_planning', 'stable_behavior'],
        'requirements': {'min_activity': 30, 'consistent_activity': True}
    },
    
    'insurance_property': {
        'name': 'Страхование имущества',
        'category': 'insurance',
        'description': 'Страхование квартиры, дома от пожара, затопления, стихийных бедствий',
        'target_users': ['home_interest', 'property_owners', 'risk_averse'],
        'requirements': {'home_interest': 0.4, 'min_activity': 25}
    },
    
    'insurance_travel': {
        'name': 'Страхование путешественников',
        'category': 'insurance',
        'description': 'Покрытие медпомощи за рубежом, отмены поездки, потери багажа',
        'target_users': ['travel_interest', 'active_lifestyle', 'frequent_travelers'],
        'requirements': {'diversity_ratio': 0.4, 'min_activity': 50}
    },
    
    # 🎯 ПАРТНЕРСКИЕ КАРТЫ
    'card_spartak': {
        'name': 'Фан-карта Спартака',
        'category': 'partner_cards',
        'description': 'Кэшбэк до 10%, эксклюзивные предложения от клуба',
        'target_users': ['sports_fans', 'loyal_customers', 'medium_activity'],
        'requirements': {'sports_interest': 0.5, 'min_activity': 40}
    },
    
    'card_lenta': {
        'name': 'Карта «Лента»',
        'category': 'partner_cards',
        'description': 'Кэшбэк до 10% на покупки в гипермаркетах «Лента»',
        'target_users': ['frequent_shoppers', 'grocery_shoppers', 'family_users'],
        'requirements': {'min_activity': 35, 'consistent_spending': True}
    },
    
    'card_sportmaster': {
        'name': 'Карта «Спортмастер»',
        'category': 'partner_cards',
        'description': 'Повышенный кэшбэк на покупки в «Спортмастере»',
        'target_users': ['sports_interest', 'active_lifestyle', 'sports_shoppers'],
        'requirements': {'sports_interest': 0.4, 'min_activity': 30}
    }
}