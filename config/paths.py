from pathlib import Path

class PathConfig:
    def __init__(self):
        self.BASE_DIR = Path("data/dataset/small")
        self.RAW_DATA = {
            'users': self.BASE_DIR / "users.pq",
            'retail_items': self.BASE_DIR / "retail/items.pq",
            'marketplace_items': self.BASE_DIR / "marketplace/items.pq",
            'offers_items': self.BASE_DIR / "offers/items.pq"
        }
        
        # Паттерны для событий
        self.EVENT_PATTERNS = {
            'retail': self.BASE_DIR / "retail/events/*.pq",
            'marketplace': self.BASE_DIR / "marketplace/events/*.pq", 
            'offers': self.BASE_DIR / "offers/events/*.pq"
        }
        
        self.PROCESSED_DIR = Path("data/processed")
        self.MODELS_DIR = Path("models")
        
    def ensure_directories(self):
        """Создание необходимых директорий"""
        self.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        self.MODELS_DIR.mkdir(parents=True, exist_ok=True)

PATHS = PathConfig()