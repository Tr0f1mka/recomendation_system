
BANK_PRODUCTS = {
    "deposits": [
        {
            "id": "deposit_1",
            "name": "Вклад «ПСБ.Накопительный»",
            "target_profile": {
                "min_balance": 1000,
                "savings_behavior": 0.7,
                "transaction_frequency": "medium"
            },
            "business_value": 0.7
        },
        {
            "id": "deposit_2",
            "name": "Вклад «ПСБ.Выгодный»",
            "target_profile": {
                "min_balance": 10000,
                "savings_behavior": 0.8,
                "transaction_frequency": "low"
            },
            "business_value": 0.8
        },
        {
            "id": "deposit_3",
            "name": "Вклад «ПСБ.Пенсионный»",
            "target_profile": {
                "age_group": "pensioner",
                "savings_behavior": 0.6,
                "transaction_frequency": "medium"
            },
            "business_value": 0.6
        },
        {
            "id": "deposit_4",
            "name": "Вклад «ПСБ.Особый»",
            "target_profile": {
                "special_status": "honored",
                "min_balance": 50000,
                "savings_behavior": 0.9
            },
            "business_value": 0.9
        }
    ],
    
    "savings_accounts": [
        {
            "id": "savings_1",
            "name": "Сберегательный счёт «ПСБ.Свободный»",
            "target_profile": {
                "min_balance": 1,
                "transaction_frequency": "high",
                "liquidity_needs": 0.9
            },
            "business_value": 0.6
        },
        {
            "id": "savings_2",
            "name": "Сберегательный счёт «ПСБ.Свободный Плюс»",
            "target_profile": {
                "digital_services_usage": 0.7,
                "transaction_frequency": "high",
                "min_balance": 1000
            },
            "business_value": 0.7
        },
        {
            "id": "savings_3",
            "name": "Сберегательный счёт для клиентов с «Почётным статусом»",
            "target_profile": {
                "special_status": "honored",
                "min_balance": 5000,
                "savings_behavior": 0.8
            },
            "business_value": 0.8
        }
    ],
    
    "premium_cards": [
        {
            "id": "premium_card_1",
            "name": "Премиальная дебетовая карта «ПСБ.Карта Premium»",
            "target_profile": {
                "min_avg_spending": 50000,
                "spending_stability": 0.7,
                "transaction_frequency": "high"
            },
            "business_value": 0.9
        }
    ],
    
    "credit_cards": [
        {
            "id": "credit_1",
            "name": "ПСБ.Кредитная карта «180 дней без %» (100 Plus)",
            "target_profile": {
                "min_avg_spending": 20000,
                "credit_affinity": 0.6,
                "transaction_frequency": "high"
            },
            "business_value": 0.8
        },
        {
            "id": "credit_2",
            "name": "Премиальная кредитная карта",
            "target_profile": {
                "min_avg_spending": 50000,
                "credit_affinity": 0.8,
                "spending_level": "high"
            },
            "business_value": 0.85
        }
    ],
    
    "debit_cards": [
        {
            "id": "debit_1",
            "name": "ПСБ.Зарплата PRO",
            "target_profile": {
                "salary_account": True,
                "transaction_frequency": "medium",
                "spending_level": "medium"
            },
            "business_value": 0.7
        },
        {
            "id": "debit_2",
            "name": "ПСБ.Карта «Из ПСБ»",
            "target_profile": {
                "transaction_frequency": "high",
                "credit_affinity": 0.5,
                "spending_level": "medium"
            },
            "business_value": 0.6
        },
        {
            "id": "debit_3",
            "name": "ПСБ.Карта «Сильные люди»",
            "target_profile": {
                "special_status": "honored",
                "salary_account": True,
                "spending_level": "high"
            },
            "business_value": 0.8
        },
        {
            "id": "debit_4",
            "name": "ПСБ.Карта «Зарплата Плюс»",
            "target_profile": {
                "salary_account": True,
                "transaction_frequency": "medium",
                "spending_level": "medium"
            },
            "business_value": 0.65
        },
        {
            "id": "debit_5",
            "name": "ПСБ.Пенсионная карта для военнослужащих",
            "target_profile": {
                "age_group": "pensioner",
                "special_status": "military",
                "transaction_frequency": "medium"
            },
            "business_value": 0.7
        },
        {
            "id": "debit_6",
            "name": "ПСБ.Пенсионная карта",
            "target_profile": {
                "age_group": "pensioner",
                "transaction_frequency": "medium",
                "spending_level": "low"
            },
            "business_value": 0.6
        },
        {
            "id": "debit_7",
            "name": "ПСБ.Карта «Только вперёд»",
            "target_profile": {
                "spending_categories": ["sports", "medicine"],
                "transaction_frequency": "medium",
                "spending_level": "medium"
            },
            "business_value": 0.65
        },
        {
            "id": "debit_8",
            "name": "ПСБ.Карта ЦСКА",
            "target_profile": {
                "loyalty_program": "cska",
                "transaction_frequency": "medium",
                "spending_level": "medium"
            },
            "business_value": 0.6
        },
        {
            "id": "debit_9",
            "name": "ПСБ.Карта резидента",
            "target_profile": {
                "resident_status": True,
                "transaction_frequency": "medium",
                "spending_level": "medium"
            },
            "business_value": 0.5
        },
        {
            "id": "debit_10",
            "name": "ПСБ.Карта «Свои»",
            "target_profile": {
                "salary_account": True,
                "partner_employee": True,
                "transaction_frequency": "medium"
            },
            "business_value": 0.7
        },
        {
            "id": "debit_11",
            "name": "ПСБ.Карта «Твой кешбэк»",
            "target_profile": {
                "transaction_frequency": "high",
                "spending_level": "medium",
                "cashback_preference": 0.8
            },
            "business_value": 0.6
        },
        {
            "id": "debit_12",
            "name": "ПСБ.Стикер «Твой кешбэк»",
            "target_profile": {
                "transaction_frequency": "high",
                "contactless_preference": 0.9,
                "spending_level": "medium"
            },
            "business_value": 0.55
        }
    ],
    
    "investment_funds": [
        {
            "id": "fund_1",
            "name": "ОПИФ «ПРОМСВЯЗЬ — Акции»",
            "target_profile": {
                "risk_tolerance": 0.7,
                "investment_experience": "intermediate",
                "min_investment": 5000
            },
            "business_value": 0.8
        },
        {
            "id": "fund_2",
            "name": "ОПИФ «ПРОМСВЯЗЬ — Фонд смешанных инвестиций»",
            "target_profile": {
                "risk_tolerance": 0.5,
                "investment_experience": "beginner",
                "min_investment": 3000
            },
            "business_value": 0.7
        },
        {
            "id": "fund_3",
            "name": "ОПИФ «ПРОМСВЯЗЬ — Облигации»",
            "target_profile": {
                "risk_tolerance": 0.3,
                "investment_experience": "beginner",
                "min_investment": 3000
            },
            "business_value": 0.6
        },
        {
            "id": "fund_4",
            "name": "ОПИФ «ПРОМСВЯЗЬ — Оборонный»",
            "target_profile": {
                "risk_tolerance": 0.8,
                "investment_experience": "advanced",
                "min_investment": 10000
            },
            "business_value": 0.85
        },
        {
            "id": "fund_5",
            "name": "ОПИФ «ПРОМСВЯЗЬ — Перспективные вложения»",
            "target_profile": {
                "risk_tolerance": 0.7,
                "investment_experience": "intermediate",
                "min_investment": 7000
            },
            "business_value": 0.75
        },
        {
            "id": "fund_6",
            "name": "ОПИФ «ПРОМСВЯЗЬ — Окно возможностей»",
            "target_profile": {
                "risk_tolerance": 0.8,
                "investment_experience": "advanced",
                "min_investment": 15000
            },
            "business_value": 0.9
        },
        {
            "id": "fund_7",
            "name": "ОПИФ «Мировой баланс»",
            "target_profile": {
                "risk_tolerance": 0.6,
                "investment_experience": "intermediate",
                "min_investment": 10000
            },
            "business_value": 0.7
        },
        {
            "id": "fund_8",
            "name": "ОПИФ «Финансовая подушка»",
            "target_profile": {
                "risk_tolerance": 0.2,
                "investment_experience": "beginner",
                "min_investment": 1000
            },
            "business_value": 0.5
        },
        {
            "id": "fund_9",
            "name": "ОПИФ «Финансовый поток»",
            "target_profile": {
                "risk_tolerance": 0.4,
                "investment_experience": "beginner",
                "min_investment": 5000
            },
            "business_value": 0.6
        },
        {
            "id": "fund_10",
            "name": "ОПИФ «Недра России»",
            "target_profile": {
                "risk_tolerance": 0.7,
                "investment_experience": "intermediate",
                "min_investment": 8000
            },
            "business_value": 0.75
        },
        {
            "id": "fund_11",
            "name": "ОПИФ «Курс на Восток»",
            "target_profile": {
                "risk_tolerance": 0.6,
                "investment_experience": "intermediate",
                "min_investment": 10000
            },
            "business_value": 0.7
        },
        {
            "id": "fund_12",
            "name": "ОПИФ «Дивидендные акции»",
            "target_profile": {
                "risk_tolerance": 0.5,
                "investment_experience": "intermediate",
                "min_investment": 6000
            },
            "business_value": 0.65
        }
    ],
    
    "premium_services": [
        {
            "id": "premium_1",
            "name": "Премиальный пакет «ПСБ.Premium»",
            "target_profile": {
                "min_avg_spending": 100000,
                "spending_stability": 0.8,
                "assets_level": "high"
            },
            "business_value": 0.95
        },
        {
            "id": "premium_2",
            "name": "Сервис «Премиум-ассистанс»",
            "target_profile": {
                "min_avg_spending": 80000,
                "travel_frequency": "high",
                "spending_level": "high"
            },
            "business_value": 0.85
        },
        {
            "id": "premium_3",
            "name": "Индивидуальное доверительное управление",
            "target_profile": {
                "assets_level": "very_high",
                "investment_experience": "advanced",
                "min_investment": 500000
            },
            "business_value": 0.9
        }
    ]
}
