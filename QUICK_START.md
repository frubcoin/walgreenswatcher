# Quick Start Guide - Walgreens Stock Watcher

## 5-Minute Setup

### Step 1: Get Your Discord Webhook URL (2 minutes)

1. Open your Discord server
2. Click on server name → Server Settings
3. Go to **Integrations** (left menu)
4. Click **Webhooks** and then **New Webhook**
5. Name it "Walgreens Watcher"
6. **Copy the Webhook URL** (looks like: `https://discord.com/api/webhooks/...`)

### Step 2: Configure the Application (1 minute)

1. Open `backend\.env.example` 
2. Copy it and rename to `backend\.env`
3. Paste your webhook URL:
   ```
   DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/YOUR_ID/YOUR_TOKEN
   ```
4. Save the file

### Step 3: Start the Application (1 minute)

**Option A - Easy (Recommended):**
- Double-click `start.bat` in the project root folder
- Wait for it to start

**Option B - Command Line:**
```powershell
cd "c:\Users\metro\Documents\walgreens codex"
.\start.bat
```

### Step 4: Open Web Interface (1 minute)

1. After startup completes, open your browser
2. Go to: **http://localhost:5000**
3. You should see a purple dashboard

## Using the App

### Starting Monitoring

1. Click the green **"▶️ Start Scheduler"** button
2. It will immediately check for stock
3. Then checks every hour automatically

### Manual Check

Click **"🔍 Check Now"** to check right away without waiting for the hourly schedule.

### Stopping

Click **"⏹️ Stop Scheduler"** to stop automatic checks.

### Viewing Results

- **Latest Results** section shows the most recent items found
- **Recent History** shows all previous checks
- **Status** shows if scheduler is running and statistics

## Discord Notifications

When stock is found, Discord will receive messages like:

```
🎉 Pokémon Card Stock Found!

📦 Product Name: 5 stores found
Stores: 3851, 3844, 4125, 2945, 5223
```

## Troubleshooting

### "Connection refused" error
- Make sure `start.bat` completed startup
- Check that browser shows: "Starting Flask app on http://localhost:5000"

### Discord not notifying
- Double-check webhook URL in `.env` file
- Make sure Discord channel permissions allow the webhook
- Check server logs for errors

### No stock found
- This might be genuine - check Walgreens.com manually
- Try clicking "Check Now" a few times
- If still nothing, it's likely truly out of stock

### Python not found
- Install Python from https://www.python.org (3.8+)
- During installation, **check "Add Python to PATH"**

## Need Help?

Check the main **README.md** file for detailed documentation.

## Common Settings

To adjust these, edit `backend/config.py`:

```python
RATE_LIMIT_DELAY = 2        # Seconds between requests (higher = slower)
CHECK_INTERVAL_HOURS = 1    # How often to check (1-24 hours)
SEARCH_RADIUS_MILES = 90    # Search radius from zip code
```

---

**Total Setup Time: ~5 minutes**

You're ready to catch those Pokémon cards! 🎉
