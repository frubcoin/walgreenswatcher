"""Database management for stock history"""
import json
import logging
import os
from typing import Dict, List, Optional
from datetime import datetime
from config import STOCK_HISTORY_FILE, DATA_DIR

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StockDatabase:
    """Manage stock history in JSON file"""
    
    def __init__(self, file_path: str = STOCK_HISTORY_FILE):
        self.file_path = file_path
        self._ensure_dir()
        self._initialize_db()
    
    def _ensure_dir(self) -> None:
        """Ensure data directory exists"""
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
    
    def _initialize_db(self) -> None:
        """Initialize database file if it doesn't exist"""
        if not os.path.exists(self.file_path):
            self._save({
                'version': 1,
                'last_check': None,
                'check_history': []
            })
    
    def _load(self) -> Dict:
        """Load database from file"""
        try:
            if os.path.exists(self.file_path):
                with open(self.file_path, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Error loading database: {str(e)}")
        
        return {
            'version': 1,
            'last_check': None,
            'check_history': []
        }
    
    def _save(self, data: Dict) -> bool:
        """Save database to file"""
        try:
            with open(self.file_path, 'w') as f:
                json.dump(data, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Error saving database: {str(e)}")
            return False
    
    def add_check_result(self, result: Dict, products_with_stock: Dict) -> bool:
        """Add a check result to history"""
        try:
            db = self._load()
            
            entry = {
                'timestamp': datetime.now().isoformat(),
                'check_result': result,
                'products_found': products_with_stock,
                'has_stock': bool(products_with_stock)
            }
            
            db['last_check'] = entry['timestamp']
            db['check_history'].append(entry)
            
            # Keep only last 1000 entries to prevent file from getting too large
            if len(db['check_history']) > 1000:
                db['check_history'] = db['check_history'][-1000:]
            
            return self._save(db)
        
        except Exception as e:
            logger.error(f"Error adding check result: {str(e)}")
            return False
    
    def get_last_check(self) -> Optional[Dict]:
        """Get the last check result"""
        db = self._load()
        check_history = db.get('check_history', [])
        
        if check_history:
            return check_history[-1]
        return None
    
    def get_recent_checks(self, limit: int = 50) -> List[Dict]:
        """Get recent check results"""
        db = self._load()
        history = db.get('check_history', [])
        return history[-limit:]
    
    def get_previous_stock_stores(self) -> Dict[str, List[str]]:
        """Get stores that had stock in previous check"""
        db = self._load()
        history = db.get('check_history', [])
        
        if history:
            last_result = history[-1]
            products_found = last_result.get('products_found', {})
            
            result = {}
            for product_id, product_info in products_found.items():
                result[product_id] = product_info.get('store_ids', [])
            
            return result
        
        return {}
    
    def get_statistics(self) -> Dict:
        """Get statistics from check history"""
        db = self._load()
        history = db.get('check_history', [])
        
        if not history:
            return {
                'total_checks': 0,
                'checks_with_stock': 0,
                'success_rate': 0,
                'last_check': None
            }
        
        checks_with_stock = sum(1 for check in history if check.get('has_stock', False))
        
        return {
            'total_checks': len(history),
            'checks_with_stock': checks_with_stock,
            'success_rate': (checks_with_stock / len(history) * 100) if history else 0,
            'last_check': db.get('last_check')
        }
