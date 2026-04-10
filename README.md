# Walgreens Stock Watcher by frub

A robust local program that monitors Walgreens store inventory for specific products and sends Discord notifications when stock is found.

## Features

✅ **Real-time Stock Monitoring**
- Automatically checks all Walgreens stores near your zip code
- Hourly scheduled checks
- Manual check capability

🤖 **Smart Rate Limiting**
- Respects Walgreens server rate limits
- Exponential backoff on failures
- Jittered request delays to avoid detection

🔔 **Discord Integration**
- Beautiful embed notifications with product details
- Store IDs and availability counts
- Color-coded status updates
- Optional notifications on check completion
- **Multiple Webhooks**: Send alerts to multiple Discord channels / servers simultaneously

💻 **Web Interface**
- Clean, modern dashboard
- **One-Click Control**: Start/Stop the monitor directly from the header or settings
- Real-time status updates
- Check history and statistics
- Easy webhook configuration
- Manual check trigger
- **Pokémon Theme Mode**: Customizable repeating backgrounds with dynamic accent colors

📊 **Data Tracking**
- Stock history database
- Check statistics
- Success rate monitoring

## Installation

### Prerequisites
- Python 3.8 or higher
- pip (Python package manager)
- A Discord server where you can create webhooks

### Step 1: Install Dependencies

Open PowerShell in the backend directory:

```powershell
pip install -r backend/requirements.txt
```

### Step 2: Configure Discord Webhook

1. **Create a Discord Webhook:**
   - Go to your Discord server
   - Server Settings → Integrations → Webhooks
   - Click "New Webhook"
   - Name it "Walgreens Watcher"
   - Copy the webhook URL

2. **Configure in the App:**
   - Launch the application (see below)
   - Open the web interface
   - Go to the **Settings** tab
   - Paste your webhook URL in the **Discord Destinations** section
   - Click **Save Settings**

## Running the Program

### 🚀 Quick Start (Windows)
Simply double-click the `start.bat` file in the root directory. This will automatically:
1. Set up a Python virtual environment (if needed).
2. Install dependencies.
3. Start the backend server.

### Manual Start
Run the following command from the root directory:

```powershell
python backend/app.py
```

### Try Scrapy + Impersonation for CVS Inventory
If you want to test CVS inventory calls through Scrapy with browser impersonation:

```powershell
python backend/cvs_inventory_spider.py --product-id skittles-original-candy-sharing-size --zip 85209
```

Notes:
- This uses `scrapy-impersonate` (`ImpersonateRequest`) and prints matching in-stock stores as JSON lines.
- `--product-id` should match the CVS PDP slug/id part used under `/shop/...`.
- You can test full browser mode with `--engine playwright` after installing browser binaries:
  - `pip install -r backend/requirements.txt`
  - `playwright install chromium`
- Install deps first with `pip install -r backend/requirements.txt` if you have not already.

### CVS Playwright Fallback On A VPS
If CVS is blocking the direct API path on your VPS, the app can now use a Playwright browser flow that follows the product page and store-check modal flow before capturing the live inventory response.

Recommended env vars:

```bash
CVS_PLAYWRIGHT_ENABLED=1
CVS_PLAYWRIGHT_FIRST=1
CVS_PLAYWRIGHT_HEADLESS=0
CVS_PLAYWRIGHT_BROWSER_EXECUTABLE_PATH=/usr/bin/chromium
CVS_PROXY_URLS=http://USERNAME:PASSWORD@HOST:PORT
```

If you want that browser path to run in a virtual display on Linux, launch the service under `xvfb-run`:

```bash
xvfb-run -a python backend/app.py
```

Useful notes:
- `CVS_PLAYWRIGHT_HEADLESS=0` is the intended setting when you are wrapping the process with `xvfb-run`.
- `CVS_PLAYWRIGHT_ONLY_MODE=1` forces the app to use the Playwright browser flow and skip the older CVS browser fallback.
- `CVS_PROXY_URLS` accepts normal authenticated proxy URLs such as `http://user:pass@host:port`.

### Decodo Web Scraping API Local Test
To test CVS inventory via Decodo Web Scraping API (instead of raw proxy requests):

```powershell
$env:DECODO_BASIC_TOKEN="YOUR_BASIC_TOKEN"
$env:DECODO_PROXY_POOL="premium"        # optional, defaults to premium
$env:DECODO_DEVICE_TYPE="desktop_chrome" # optional
# Optional sticky session for multi-step consistency:
# $env:DECODO_SESSION_ID="cvs-318928-1"
python backend/decodo_cvs_inventory_test.py --product-id 318928 --zip 85208 --poll-seconds 60
```

The script prints Decodo status fields plus whether an inventory response block was returned.

You'll see output like:
```
==================================================
Starting Flask app on http://localhost:5000
Open http://localhost:5000 in your browser
==================================================
```

### Access the Web Interface

Open your browser and go to:
```
http://localhost:5000
```

## How to Use

### Initial Setup

1. **Launch the app** (see Running the Program above)
2. **Configure Settings**:
   - Open `http://localhost:5000`
   - Switch to the **Settings** tab
   - Enter your **ZIP Code** (default is 85209)
   - Add your **Discord Webhooks**
   - Use the **Add Product From Link** feature to track specific items (like Pokemon cards)
   - Click **Save Settings**
3. **Start the Monitor**:
   - Click the **Start** button in the header or the settings panel
   - The monitor will immediately perform the first check
   - Then checks will run every hour (or your configured interval) automatically

### Manual Checks

Click the **Check Now** button in the header anytime to perform an immediate stock check without waiting for the scheduled interval.

### Dashboard

The dashboard shows:
- **Status**: Whether the scheduler is running
- **Last Check**: When the most recent check occurred
- **Tracked Products**: Quick count of items being watched
- **Results**: Live view of stock found near your location
- **Progress**: Watch the live check progress bar when a scan is in flight

### Stopping

Click the **Stop** button in the header to stop automatic checks. You can still perform manual checks afterward.

## Key Design Decisions (Rate Limiting & Reliability)

### Rate Limiting Strategy
- **2-second delay** between requests (configurable in config.py) 
- **Jitter (±20%)** on delays to avoid predictable patterns
- **Exponential backoff** on failures (1s, 1.5s, 2.25s)
- **Max 3 retries** per request before failing
- **User-Agent rotation** between realistic browser strings
- **Session reuse** to maintain connection pooling

### Why This Approach
- Walgreens actively detects and blocks aggressive scrapers
- Real browsers do 2-5 second gaps between requests
- Jitter prevents detection patterns
- Exponential backoff prevents hammering on errors
- Multiple retries handle occasional network issues

### Reliability Features
- **Comprehensive logging** for debugging
- **Error recovery** with graceful degradation
- **State persistence** - checks don't get lost
- **Database tracking** - can review all previous checks
- **Discord error notifications** if something fails

## Configuration

You can customize core behavior in `backend/config.py`:

```python
# Location settings
TARGET_ZIP_CODE = '55555'           # Change default zip code
SEARCH_RADIUS_MILES = 20            # Change search radius

# Schedule
DEFAULT_CHECK_INTERVAL_MINUTES = 60  # Change check frequency

# Rate limiting
RATE_LIMIT_DELAY = 2                # Seconds between requests
MAX_RETRIES = 3                     # Retry attempts
RETRY_BACKOFF = 1.5                 # Backoff multiplier
```

## Troubleshooting

### "Connection Refused" Error
The Flask app isn't running. Make sure you started it:
```powershell
python backend/app.py
```

### Discord Notifications Not Sending
1. Verify the webhook URL in the app Settings is correct
2. Check the Discord channel permissions
3. Look at the console logs for errors
4. Try a manual check and watch console output

### No Stock Found
This could be genuine - not all stores have stock. To verify:
1. Manually check Walgreens.com for the products
2. Check console logs in the Flask app for network errors
3. Verify your ZIP code is correct

### Rate Limiting Issues (429 Errors)
If you get "429 Too Many Requests" errors:
1. Increase `RATE_LIMIT_DELAY` in config.py (try 3-5 seconds)
2. Restart the application after changes

## Architecture

```
walgreens-codex/
├── backend/
│   ├── app.py                 # Flask application & API
│   ├── config.py              # Configuration
│   ├── walgreens_scraper.py   # Stock checking logic
│   ├── rate_limiter.py        # Rate limiting & retry logic
│   ├── discord_notifier.py    # Discord notifications
│   ├── scheduler.py           # Background monitoring
│   └── database.py            # Persistence layer
├── frontend/
│   ├── index.html             # UI (HTML/CSS/JS)
│   └── *.webp                 # Asset icons & images
└── data/
    └── stock_history.json     # Local database
```

## Disclaimer

This tool is for personal use only. Use responsibly and respect Walgreens' terms of service. The rate limiting is designed to avoid causing server load; do not increase request frequency excessively.
