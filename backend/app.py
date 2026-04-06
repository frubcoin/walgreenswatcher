"""Flask application for Walgreens Stock Watcher"""
import sys
import os

# Add backend directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from datetime import datetime
import logging
from scheduler import StockCheckScheduler
from database import StockDatabase
from walgreens_product_resolver import WalgreensProductResolver

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Initialize components
scheduler = None
db = StockDatabase()

def init_scheduler(webhook_url=''):
    """Initialize the scheduler"""
    global scheduler
    if scheduler is None:
        scheduler = StockCheckScheduler(webhook_url)
    return scheduler

# ==================== API Routes ====================

@app.route('/api/status', methods=['GET'])
def get_status():
    """Get current status of the checker"""
    if scheduler is None:
        return jsonify({'error': 'Scheduler not initialized'}), 500
    
    status = scheduler.get_status()
    stats = db.get_statistics()
    
    return jsonify({
        'status': status,
        'statistics': stats
    })

@app.route('/api/check', methods=['POST'])
def manual_check():
    """Perform a manual stock check"""
    global scheduler
    
    if scheduler is None:
        logger.info("Initializing scheduler for manual check...")
        init_scheduler()
    
    logger.info("Manual check endpoint called")
    result = scheduler.manual_check()
    return jsonify(result)

@app.route('/api/progress', methods=['GET'])
def get_progress():
    """Get current check progress"""
    if scheduler is None:
        return jsonify({'error': 'Scheduler not initialized'}), 500
    
    progress = scheduler.get_progress()
    return jsonify(progress)

@app.route('/api/start', methods=['POST'])
def start_scheduler():
    """Start the scheduler"""
    data = request.json or {}
    webhook_value = data.get('discord_destinations')
    if webhook_value is None:
        webhook_value = data.get('webhook_urls')
    if webhook_value is None:
        webhook_value = data.get('webhook_url', '')

    if scheduler is None:
        init_scheduler(webhook_value)
    elif webhook_value:
        scheduler.set_discord_destinations(webhook_value)

    interval_minutes = data.get('check_interval_minutes')
    if interval_minutes is not None:
        try:
            scheduler.set_check_interval_minutes(interval_minutes)
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400
    
    success = scheduler.start()
    
    if success:
        return jsonify({'message': 'Scheduler started', 'success': True})
    else:
        return jsonify({'message': 'Scheduler already running', 'success': False}), 400

@app.route('/api/stop', methods=['POST'])
def stop_scheduler():
    """Stop the scheduler"""
    if scheduler is None:
        return jsonify({'error': 'Scheduler not initialized'}), 500
    
    success = scheduler.stop()
    
    if success:
        return jsonify({'message': 'Scheduler stopped', 'success': True})
    else:
        return jsonify({'error': 'Scheduler not running'}, 400)

@app.route('/api/history', methods=['GET'])
def get_history():
    """Get recent check history"""
    limit = request.args.get('limit', default=50, type=int)
    history = db.get_recent_checks(limit)
    
    return jsonify({
        'history': history,
        'count': len(history)
    })

@app.route('/api/last-check', methods=['GET'])
def get_last_check():
    """Get the last check result"""
    last = db.get_last_check()
    
    if last:
        return jsonify(last)
    else:
        return jsonify({'message': 'No checks performed yet'}), 404

@app.route('/api/configure', methods=['POST'])
def configure():
    """Configure persisted app settings."""
    data = request.json or {}
    webhook_value = data.get('discord_destinations')
    if webhook_value is None:
        webhook_value = data.get('webhook_urls')
    if webhook_value is None:
        webhook_value = data.get('webhook_url')
    zipcode = data.get('zipcode')
    interval_minutes = data.get('check_interval_minutes')
    pokemon_background_enabled = data.get('pokemon_background_enabled')
    pokemon_background_theme = data.get('pokemon_background_theme')
    pokemon_background_tile_size = data.get('pokemon_background_tile_size')
    
    if scheduler is None:
        # Initialize with webhook if provided
        init_scheduler(webhook_value or '')
    
    # Update webhook if provided
    if webhook_value is not None:
        scheduler.set_discord_destinations(webhook_value)
    
    # Update ZIP code if provided
    if zipcode is not None:
        scheduler.set_zipcode(zipcode)

    # Update check interval if provided
    if interval_minutes is not None:
        try:
            scheduler.set_check_interval_minutes(interval_minutes)
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400

    if pokemon_background_enabled is not None:
        try:
            scheduler.set_pokemon_background_enabled(pokemon_background_enabled)
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400

    if pokemon_background_theme is not None:
        try:
            scheduler.set_pokemon_background_theme(pokemon_background_theme)
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400

    if pokemon_background_tile_size is not None:
        try:
            scheduler.set_pokemon_background_tile_size(pokemon_background_tile_size)
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400
    
    return jsonify({
        'message': 'Configuration updated',
        'discord_configured': scheduler.notifier.is_configured,
        'discord_webhook_count': len(scheduler.notifier.webhook_urls),
        'discord_destinations': scheduler.discord_destinations,
        'zipcode': scheduler.current_zipcode,
        'check_interval_minutes': scheduler.check_interval_minutes,
        'pokemon_background_enabled': scheduler.pokemon_background_enabled,
        'pokemon_background_theme': scheduler.pokemon_background_theme,
        'pokemon_background_tile_size': scheduler.pokemon_background_tile_size,
    })

@app.route('/api/products/add', methods=['POST'])
def add_product():
    """Add a new product to track"""
    if scheduler is None:
        init_scheduler()
    
    data = request.json or {}
    product_link = data.get('url', '').strip()
    custom_name = data.get('name', '').strip()
    
    if product_link:
        try:
            resolved = WalgreensProductResolver.resolve_product_link(product_link)
        except Exception as exc:
            return jsonify({'error': str(exc)}), 400

        product_id = resolved['article_id']
        product_name = custom_name or resolved['name']
        planogram = resolved['planogram']
        image_url = resolved.get('image_url', '')
        source_url = resolved.get('canonical_url', product_link)
        resolved_product_id = resolved.get('product_id', '')
    else:
        product_id = data.get('id', '').strip()
        product_name = custom_name
        planogram = data.get('planogram', '').strip()
        image_url = data.get('image_url', '').strip()
        source_url = data.get('source_url', '').strip()
        resolved_product_id = data.get('product_id', '').strip()

        if not product_id or not product_name or not planogram:
            return jsonify({'error': 'Product URL or product ID, name, and planogram are required'}), 400

    success = scheduler.add_product(
        product_id,
        product_name,
        planogram,
        image_url=image_url,
        source_url=source_url,
        product_id=resolved_product_id,
    )
    
    if success:
        return jsonify({
            'message': 'Product added',
            'id': product_id,
            'name': product_name,
            'planogram': planogram,
            'image_url': image_url,
            'source_url': source_url or None,
            'product_id': resolved_product_id or None,
        })
    else:
        return jsonify({'error': 'Product already tracked'}), 400

@app.route('/api/products/resolve', methods=['POST'])
def resolve_product():
    """Resolve a Walgreens product link into inventory metadata"""
    data = request.json or {}
    product_link = data.get('url', '').strip()

    if not product_link:
        return jsonify({'error': 'Product URL required'}), 400

    try:
        resolved = WalgreensProductResolver.resolve_product_link(product_link)
    except Exception as exc:
        return jsonify({'error': str(exc)}), 400

    return jsonify(resolved)

@app.route('/api/products/remove', methods=['POST'])
def remove_product():
    """Remove a product from tracking"""
    if scheduler is None:
        return jsonify({'error': 'Scheduler not initialized'}), 500
    
    data = request.json
    product_id = data.get('id', '').strip()
    
    if not product_id:
        return jsonify({'error': 'Product ID required'}), 400
    
    success = scheduler.remove_product(product_id)
    
    if success:
        return jsonify({'message': 'Product removed', 'id': product_id})
    else:
        return jsonify({'error': 'Product not found'}), 404

@app.route('/api/products/update', methods=['POST'])
def update_product():
    """Update a tracked product"""
    if scheduler is None:
        init_scheduler()

    data = request.json or {}
    product_id = data.get('id', '').strip()
    product_name = data.get('name', '').strip()

    if not product_id:
        return jsonify({'error': 'Product ID required'}), 400

    if not product_name:
        return jsonify({'error': 'Product name required'}), 400

    try:
        success = scheduler.update_product_name(product_id, product_name)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400

    if success:
        return jsonify({'message': 'Product updated', 'id': product_id, 'name': product_name})
    else:
        return jsonify({'error': 'Product not found'}), 404

# ==================== Static Files ====================

@app.route('/')
def index():
    """Serve the web interface"""
    return send_from_directory('../frontend', 'index.html')

@app.route('/<path:path>')
def serve_static(path):
    """Serve static files"""
    return send_from_directory('../frontend', path)

# ==================== Health Check ====================

@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'ok', 'timestamp': datetime.now().isoformat()})

# ==================== Error Handlers ====================

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(500)
def server_error(error):
    logger.error(f"Server error: {error}")
    return jsonify({'error': 'Internal server error'}), 500

# ==================== Startup ====================

if __name__ == '__main__':
    # Initialize scheduler
    init_scheduler()
    
    logger.info("=" * 50)
    logger.info("Walgreens Stock Watcher")
    logger.info("=" * 50)
    logger.info("Starting Flask app on http://localhost:5000")
    logger.info("Open http://localhost:5000 in your browser")
    logger.info("=" * 50)
    
    # Run app
    app.run(debug=False, host='0.0.0.0', port=5000, use_reloader=False)
