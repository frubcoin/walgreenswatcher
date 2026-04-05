// API Configuration
const API_BASE = 'http://localhost:5000/api';

// Update interval (5 seconds)
const UPDATE_INTERVAL = 5000;

// State
let updateInterval = null;

// ==================== Initialization ====================

document.addEventListener('DOMContentLoaded', () => {
    console.log('Initializing Walgreens Stock Watcher...');
    
    // Initial status load
    loadStatus();
    
    // Set up auto-refresh
    updateInterval = setInterval(loadStatus, UPDATE_INTERVAL);
    
    // Load initial history
    loadHistory();
});

// ==================== API Calls ====================

async function apiCall(endpoint, method = 'GET', data = null) {
    try {
        const options = {
            method: method,
            headers: {
                'Content-Type': 'application/json',
            }
        };
        
        if (data) {
            options.body = JSON.stringify(data);
        }
        
        const response = await fetch(`${API_BASE}${endpoint}`, options);
        
        if (!response.ok) {
            throw new Error(`API error: ${response.status}`);
        }
        
        return await response.json();
    } catch (error) {
        console.error(`Error calling ${endpoint}:`, error);
        showError(`Error calling API: ${error.message}`);
        return null;
    }
}

// ==================== Status Management ====================

async function loadStatus() {
    const result = await apiCall('/status');
    
    if (!result) return;
    
    const status = result.status || {};
    const stats = result.statistics || {};
    
    // Update ZIP code display in form
    const currentZipcode = status.current_zipcode || '85209';
    const zipInput = document.getElementById('zipCode');
    if (zipInput) {
        zipInput.placeholder = `Current: ${currentZipcode}`;
        if (!zipInput.value) {
            zipInput.value = currentZipcode;
        }
    }
    
    // Update scheduler status
    const isRunning = status.is_running || false;
    document.getElementById('schedulerStatus').textContent = 
        isRunning ? '🟢 Running' : '🔴 Stopped';
    
    // Update last check time
    const lastCheck = status.last_check;
    if (lastCheck) {
        const date = new Date(lastCheck);
        document.getElementById('lastCheckTime').textContent = date.toLocaleString();
    }
    
    // Update Discord status
    const discordConfigured = status.discord_configured || false;
    document.getElementById('discordStatus').textContent = 
        discordConfigured ? '✅ Connected' : '❌ Not Configured';
    
    // Update statistics
    document.getElementById('totalChecks').textContent = stats.total_checks || 0;
    document.getElementById('stockFoundCount').textContent = stats.checks_with_stock || 0;
    document.getElementById('successRate').textContent = 
        (stats.success_rate || 0).toFixed(1) + '%';
    
    // Update status indicator
    const indicator = document.getElementById('statusIndicator');
    const statusText = document.getElementById('statusText');
    
    if (isRunning) {
        indicator.classList.add('active');
        indicator.classList.remove('error');
        statusText.textContent = 'Scheduler Running';
    } else if (status.last_check) {
        indicator.classList.remove('active');
        indicator.classList.remove('error');
        statusText.textContent = 'Ready to Check';
    } else {
        indicator.classList.remove('active');
        indicator.classList.add('error');
        statusText.textContent = 'Not Configured';
    }
    
    // Update last results
    updateLastResults(status.last_products_found || {});
}

async function updateLastResults(productsFound) {
    const container = document.getElementById('resultsContainer');
    
    if (!productsFound || Object.keys(productsFound).length === 0) {
        container.innerHTML = '<p class="empty-state">No stock found in latest check.</p>';
        return;
    }
    
    let html = '';
    
    for (const [productId, productInfo] of Object.entries(productsFound)) {
        const productName = productInfo.product_name || 'Unknown Product';
        const storeIds = productInfo.store_ids || [];
        const count = storeIds.length;
        
        html += `
            <div class="result-card">
                <h3>✅ ${productName}</h3>
                <p><strong>Stores Found:</strong> ${count}</p>
                <div class="store-ids">
                    <strong>Store IDs:</strong><br>
                    ${storeIds.join(', ')}
                </div>
            </div>
        `;
    }
    
    container.innerHTML = html;
}

// ==================== Controls ====================

async function startScheduler() {
    console.log('Starting scheduler...');
    showLoading('Starting scheduler...');
    
    const result = await apiCall('/start', 'POST', {});
    
    if (result) {
        showSuccess('Scheduler started! Will check every hour.');
        await loadStatus();
    } else {
        showError('Failed to start scheduler');
    }
}

async function stopScheduler() {
    console.log('Stopping scheduler...');
    showLoading('Stopping scheduler...');
    
    const result = await apiCall('/stop', 'POST', {});
    
    if (result) {
        showSuccess('Scheduler stopped.');
        await loadStatus();
    } else {
        showError('Failed to stop scheduler');
    }
}

async function manualCheck() {
    console.log('Performing manual check...');
    showLoading('Checking stock... This may take a minute.');
    
    const result = await apiCall('/check', 'POST', {});
    
    if (result && result.success) {
        showSuccess('✅ Manual check completed!');
        await loadStatus();
        await loadHistory();
    } else {
        showError('Manual check did not complete successfully');
    }
}

async function configureWebhook() {
    const webhookUrl = document.getElementById('webhookUrl').value.trim();
    
    if (!webhookUrl) {
        showError('Webhook URL is required');
        return;
    }
    
    console.log('Configuring Discord webhook...');
    showLoading('Saving webhook URL...');
    
    const result = await apiCall('/configure', 'POST', {
        webhook_url: webhookUrl
    });
    
    if (result) {
        showSuccess('✅ Discord webhook configured successfully!');
        document.getElementById('webhookUrl').value = '';
        await loadStatus();
    } else {
        showError('Failed to configure webhook');
    }
}

async function updateZipCode() {
    const zipCode = document.getElementById('zipCode').value.trim();
    
    if (!zipCode) {
        showZipError('ZIP code is required');
        return;
    }
    
    if (!/^\d{5}(-\d{4})?$/.test(zipCode)) {
        showZipError('Invalid ZIP code format (use 5 digits)');
        return;
    }
    
    console.log('Updating ZIP code to:', zipCode);
    showZipLoading('Updating search location...');
    
    const result = await apiCall('/configure', 'POST', {
        zipcode: zipCode
    });
    
    if (result) {
        showZipSuccess('✅ ZIP code updated successfully!');
        document.getElementById('zipCode').value = zipCode;
        await loadStatus();
        // Clear results to show they're from old search
        document.getElementById('resultsContainer').innerHTML = 
            '<p class="empty-state">Search location updated. Click "Check Now" to find stock in the new area.</p>';
    } else {
        showZipError('Failed to update ZIP code');
    }
}

// ==================== History ====================

async function loadHistory() {
    const result = await apiCall('/history?limit=10');
    
    if (!result || !result.history) return;
    
    const container = document.getElementById('historyContainer');
    const history = result.history;
    
    if (history.length === 0) {
        container.innerHTML = '<p class="empty-state">No history available yet.</p>';
        return;
    }
    
    let html = '';
    
    for (const item of history.slice().reverse()) {
        const timestamp = new Date(item.timestamp);
        const hasStock = item.has_stock || false;
        const productsFound = item.products_found || {};
        
        let detail = hasStock 
            ? `✅ Found ${Object.keys(productsFound).length} product(s)`
            : '❌ No stock';
        
        const itemClass = hasStock ? 'with-stock' : '';
        
        html += `
            <div class="history-item ${itemClass}">
                <div class="history-time">${timestamp.toLocaleString()}</div>
                <div class="history-detail">${detail}</div>
            </div>
        `;
    }
    
    container.innerHTML = html;
}

// ==================== UI Helpers ====================

function showSuccess(message) {
    const statusDiv = document.getElementById('webhookStatus');
    statusDiv.textContent = message;
    statusDiv.className = 'status-small success';
    setTimeout(() => {
        statusDiv.className = 'status-small';
    }, 5000);
}

function showError(message) {
    const statusDiv = document.getElementById('webhookStatus');
    statusDiv.textContent = `❌ ${message}`;
    statusDiv.className = 'status-small error';
    setTimeout(() => {
        statusDiv.className = 'status-small';
    }, 5000);
}

function showLoading(message) {
    const statusDiv = document.getElementById('webhookStatus');
    statusDiv.textContent = `⏳ ${message}`;
    statusDiv.className = 'status-small loading';
}

function showZipSuccess(message) {
    const statusDiv = document.getElementById('zipStatus');
    if (!statusDiv) return;
    statusDiv.textContent = message;
    statusDiv.className = 'status-small success';
    setTimeout(() => {
        statusDiv.className = 'status-small';
    }, 5000);
}

function showZipError(message) {
    const statusDiv = document.getElementById('zipStatus');
    if (!statusDiv) return;
    statusDiv.textContent = `❌ ${message}`;
    statusDiv.className = 'status-small error';
    setTimeout(() => {
        statusDiv.className = 'status-small';
    }, 5000);
}

function showZipLoading(message) {
    const statusDiv = document.getElementById('zipStatus');
    if (!statusDiv) return;
    statusDiv.textContent = `⏳ ${message}`;
    statusDiv.className = 'status-small loading';
}

// Auto-refresh on visibility
document.addEventListener('visibilitychange', () => {
    if (document.visible === false) {
        clearInterval(updateInterval);
    } else {
        loadStatus();
        updateInterval = setInterval(loadStatus, UPDATE_INTERVAL);
    }
});
