const runtimeConfig = window.WATCHER_RUNTIME_CONFIG || {};
const apiBase = String(runtimeConfig.apiBaseUrl || window.location.origin).replace(/\/+$/, '');
const statusBanner = document.getElementById('status-banner');

const state = {
  session: null,
  overview: null,
  userSearch: '',
  userFilter: 'all',
  eventFilter: 'all'
};

let bannerTimer = 0;

function apiUrl(path) {
  return path.startsWith('http') ? path : `${apiBase}${path}`;
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function formatDateTime(value) {
  if (!value) return 'Unknown time';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleString();
}

function formatPercent(value) {
  const numeric = Number(value || 0);
  return `${numeric.toFixed(1)}%`;
}

function formatInterval(value) {
  const minutes = Math.max(1, Number(value || 0));
  if (minutes % 60 === 0) {
    const hours = minutes / 60;
    return hours === 1 ? 'Every 1 hour' : `Every ${hours} hours`;
  }
  return `Every ${minutes} min`;
}

function showBanner(message, tone = 'info') {
  window.clearTimeout(bannerTimer);
  statusBanner.textContent = message;
  statusBanner.dataset.tone = tone;
  statusBanner.classList.add('is-visible');
  bannerTimer = window.setTimeout(() => {
    statusBanner.classList.remove('is-visible');
  }, 4200);
}

async function apiRequest(path, method = 'GET', body = null) {
  const response = await fetch(apiUrl(path), {
    method,
    credentials: 'include',
    headers: { 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : null
  });

  let payload = null;
  try {
    payload = await response.json();
  } catch (error) {
  }

  if (!response.ok) {
    const message = payload?.error || `Request failed (${response.status})`;
    const err = new Error(message);
    err.status = response.status;
    throw err;
  }

  return payload;
}

function setGoogleStatus(message, tone = 'info') {
  const element = document.getElementById('admin-google-status');
  element.textContent = message || '';
  element.dataset.tone = tone;
}

function isUserNew(user) {
  if (!user?.created_at) return false;
  return Date.now() - new Date(user.created_at).getTime() < 1000 * 60 * 60 * 24 * 3;
}

function userNeedsAttention(user) {
  return Boolean(user?.is_banned || !user?.is_authorized_email || user?.scheduler_enabled);
}

function setSessionUi(session) {
  const authPanel = document.getElementById('admin-auth-panel');
  const dashboard = document.getElementById('admin-dashboard');
  const logoutButton = document.getElementById('logout-button');
  const sessionChip = document.getElementById('admin-session-chip');
  const authCopy = document.getElementById('admin-auth-copy');
  const googleCard = document.getElementById('admin-google-user-card');
  const googleName = document.getElementById('admin-google-user-name');
  const googleEmail = document.getElementById('admin-google-user-email');
  const googleCopy = document.getElementById('admin-google-copy');
  const googleSignoutButton = document.getElementById('google-signout-button');
  const passwordInput = document.getElementById('admin-password');
  const passwordSubmit = document.getElementById('admin-password-submit');

  if (!session.configured) {
    authPanel.hidden = false;
    dashboard.hidden = true;
    logoutButton.hidden = true;
    sessionChip.textContent = 'Admin password missing';
    authCopy.textContent = 'Set ADMIN_PANEL_PASSWORD on the backend before this panel can be used.';
    googleCard.hidden = true;
    googleSignoutButton.hidden = true;
    passwordInput.disabled = true;
    passwordSubmit.disabled = true;
    setGoogleStatus('Admin password is not configured on the backend.', 'error');
    return;
  }

  const googleAuthenticated = Boolean(session.google_authenticated && session.user);
  const fullyAuthenticated = Boolean(session.authenticated);

  authPanel.hidden = fullyAuthenticated;
  dashboard.hidden = !fullyAuthenticated;
  logoutButton.hidden = !googleAuthenticated;

  if (fullyAuthenticated) {
    sessionChip.textContent = `Admin unlocked | ${session.user.email}`;
    authCopy.textContent = 'Admin access is active for this session.';
    setGoogleStatus('Google verified and admin password accepted.', 'success');
  } else if (googleAuthenticated) {
    sessionChip.textContent = `Google verified | ${session.user.email}`;
    authCopy.textContent = 'Google sign-in is complete. Enter the admin password to unlock admin access.';
    setGoogleStatus('Google sign-in complete. Continue with the admin password.', 'success');
  } else {
    sessionChip.textContent = 'Locked';
    authCopy.textContent = 'Sign in with an approved Google account first, then enter the server-side admin password to unlock moderation controls.';
    if (session.access_denied_reason) {
      setGoogleStatus(session.access_denied_reason, 'error');
    } else if (!session.google_client_id) {
      setGoogleStatus('GOOGLE_CLIENT_ID is not configured on the backend.', 'error');
    } else {
      setGoogleStatus('Use one of the approved Google emails for this app.', 'info');
    }
  }

  if (googleAuthenticated) {
    googleCard.hidden = false;
    googleName.textContent = session.user.name || session.user.email;
    googleEmail.textContent = session.user.email || '';
    googleCopy.textContent = 'Your current Google session is checked against the approval list before admin unlock.';
  } else {
    googleCard.hidden = true;
    googleName.textContent = '';
    googleEmail.textContent = '';
    googleCopy.textContent = 'Use one of the approved Google emails for this app.';
  }

  googleSignoutButton.hidden = !googleAuthenticated;
  passwordInput.disabled = !googleAuthenticated;
  passwordSubmit.disabled = !googleAuthenticated;
}

function renderPlatformSnapshot(platform) {
  const globalStats = platform?.global_statistics || {};
  const uptime = platform?.service_uptime || {};
  const totals = platform?.totals || {};

  document.getElementById('hero-uptime-label').textContent = uptime.label || 'Service uptime unavailable';
  document.getElementById('hero-users-total').textContent = `${totals.users || 0} users`;
  document.getElementById('hero-summary-copy').textContent =
    `${totals.scheduler_enabled_users || 0} user schedulers are active, ${totals.login_denials || 0} recent login denials were recorded, and ${totals.alert_webhooks || 0} admin alert webhooks are configured.`;

  const metrics = [
    {
      label: 'Uptime',
      value: formatPercent(uptime.uptime_percentage || 0),
      note: uptime.label || `${uptime.tracked_minutes || 0} minutes tracked`
    },
    {
      label: 'Total Checks',
      value: String(globalStats.total_checks || 0),
      note: `${globalStats.successful_checks || 0} successful checks recorded`
    },
    {
      label: 'Login Denials',
      value: String(totals.login_denials || 0),
      note: 'Recent blocked sign-in attempts'
    },
    {
      label: 'Schedulers',
      value: String(totals.scheduler_enabled_users || 0),
      note: `${totals.banned_users || 0} banned | ${totals.authorized_users || 0} approved`
    }
  ];

  document.getElementById('platform-metrics-strip').innerHTML = metrics.map(metric => `
    <article class="metric-card">
      <div class="metric-label">${escapeHtml(metric.label)}</div>
      <div class="metric-value">${escapeHtml(metric.value)}</div>
      <div class="metric-note">${escapeHtml(metric.note)}</div>
    </article>
  `).join('');
}

function buildAttentionItems(overview) {
  const users = overview?.users || [];
  const totals = overview?.platform?.totals || {};
  const unauthorizedUsers = users.filter(user => !user.is_authorized_email);
  const runningUnauthorized = unauthorizedUsers.filter(user => user.scheduler_enabled);
  const recentlyJoined = users.filter(user => isUserNew(user));

  const items = [];

  if (runningUnauthorized.length) {
    items.push({
      title: `${runningUnauthorized.length} scheduler account${runningUnauthorized.length === 1 ? '' : 's'} need approval review`,
      body: 'These users still have schedulers enabled while their email is waiting for approval.',
      tone: 'warning'
    });
  }

  if (totals.login_denials) {
    items.push({
      title: `${totals.login_denials} recent login denial${totals.login_denials === 1 ? '' : 's'}`,
      body: 'Review the event stream to confirm whether they were expected approval or ban decisions.',
      tone: 'danger'
    });
  }

  if (recentlyJoined.length) {
    items.push({
      title: `${recentlyJoined.length} new user${recentlyJoined.length === 1 ? '' : 's'} in the last 72 hours`,
      body: 'Verify their access posture and decide whether they should remain on the platform.',
      tone: 'success'
    });
  }

  if (!items.length) {
    items.push({
      title: 'No active operator queue',
      body: 'Nothing urgent is standing out right now. The platform appears stable.',
      tone: 'success'
    });
  }

  return items;
}

function renderAttentionQueue(overview) {
  const items = buildAttentionItems(overview);
  const container = document.getElementById('attention-list');
  container.innerHTML = items.map(item => `
    <article class="attention-item">
      <div class="tag-row">
        <span class="chip ${item.tone === 'danger' ? 'chip-danger' : item.tone === 'warning' ? 'chip-warning' : 'chip-success'}">${escapeHtml(item.tone)}</span>
      </div>
      <strong>${escapeHtml(item.title)}</strong>
      <p>${escapeHtml(item.body)}</p>
    </article>
  `).join('');
}

function renderSettings(settings) {
  document.getElementById('allowlist-enabled').checked = true;
  document.getElementById('allowlist-enabled').disabled = true;
  document.getElementById('alert-new-users').checked = Boolean(settings.alert_new_users);
  document.getElementById('alert-user-actions').checked = Boolean(settings.alert_user_actions);
  document.getElementById('admin-webhooks').value = (settings.admin_webhook_destinations || [])
    .map(destination => destination.url || '')
    .filter(Boolean)
    .join('\n');
}

function renderAuthorizedEmails(entries) {
  const container = document.getElementById('authorized-email-list');
  if (!entries.length) {
    container.innerHTML = '<div class="empty-state">No approved Google emails have been added yet.</div>';
    return;
  }

  container.innerHTML = entries.map(entry => `
    <article class="allowlist-item">
      <div class="allowlist-address">
        <strong>${escapeHtml(entry.email)}</strong>
        <span class="allowlist-note">${escapeHtml(entry.note || 'No note')} | Added ${escapeHtml(formatDateTime(entry.added_at))}</span>
      </div>
      <button class="button button-danger button-compact" type="button" data-action="remove-authorized-email" data-email="${escapeHtml(entry.email)}">Remove</button>
    </article>
  `).join('');
}

function filterUsers(users) {
  const search = state.userSearch.trim().toLowerCase();
  let filtered = users.slice();

  if (state.userFilter === 'attention') {
    filtered = filtered.filter(userNeedsAttention);
  } else if (state.userFilter === 'banned') {
    filtered = filtered.filter(user => user.is_banned);
  } else if (state.userFilter === 'unauthorized') {
    filtered = filtered.filter(user => !user.is_authorized_email);
  } else if (state.userFilter === 'scheduler') {
    filtered = filtered.filter(user => user.scheduler_enabled);
  } else if (state.userFilter === 'new') {
    filtered = filtered.filter(isUserNew);
  }

  if (search) {
    filtered = filtered.filter(user => [
      user.name,
      user.email,
      user.current_zipcode,
      user.ban_reason,
      ...(user.tracked_product_names || [])
    ].join(' ').toLowerCase().includes(search));
  }

  return filtered;
}

function renderUsers(users) {
  const filtered = filterUsers(users);
  const container = document.getElementById('user-list');
  if (!filtered.length) {
    container.innerHTML = '<div class="empty-state">No users match the current filter.</div>';
    return;
  }

  container.innerHTML = filtered.map(user => `
    <article class="user-card ${user.is_banned ? 'is-banned' : ''} ${userNeedsAttention(user) ? 'needs-attention' : ''}">
      <div class="user-card-head">
        <div class="user-title">
          <strong>${escapeHtml(user.name || user.email)}</strong>
          <div class="user-meta">${escapeHtml(user.email)} | Joined ${escapeHtml(formatDateTime(user.created_at))} | Last login ${escapeHtml(formatDateTime(user.last_login_at))}</div>
          <div class="user-meta">Monitor cadence: ${escapeHtml(formatInterval(user.check_interval_minutes || 60))}</div>
        </div>
        <div class="tag-row">
          <span class="chip ${user.is_banned ? 'chip-danger' : 'chip-success'}">${user.is_banned ? 'Banned' : 'Active'}</span>
          <span class="chip ${user.is_authorized_email ? 'chip-success' : 'chip-warning'}">${user.is_authorized_email ? 'Approved' : 'Waiting approval'}</span>
          <span class="chip ${user.scheduler_enabled ? 'chip-warning' : ''}">${user.scheduler_enabled ? 'Scheduler on' : 'Scheduler off'}</span>
          ${isUserNew(user) ? '<span class="chip chip-success">New user</span>' : ''}
        </div>
      </div>

      <div class="user-stats">
        <div class="user-stat"><strong>${escapeHtml(String(user.tracked_product_count || 0))}</strong><span>Tracked products</span></div>
        <div class="user-stat"><strong>${escapeHtml(String(user.total_checks || 0))}</strong><span>Total checks</span></div>
        <div class="user-stat"><strong>${escapeHtml(user.current_zipcode || 'None')}</strong><span>ZIP code</span></div>
        <div class="user-stat"><strong>${escapeHtml(formatInterval(user.check_interval_minutes || 60))}</strong><span>Check interval</span></div>
        <div class="user-stat"><strong>${escapeHtml(formatDateTime(user.last_check))}</strong><span>Last check</span></div>
      </div>

      <div class="user-products-block">
        <div class="user-products-head">
          <strong>Tracked products</strong>
          <span>${escapeHtml(String(user.tracked_product_count || 0))}</span>
        </div>
        ${user.tracked_product_names?.length
          ? `<div class="user-products-list">${user.tracked_product_names.map(name => `<span class="meta-tag">${escapeHtml(name)}</span>`).join('')}</div>`
          : '<div class="user-products-empty">No tracked products yet.</div>'}
      </div>

      ${user.ban_reason ? `<div class="user-meta">Ban reason: ${escapeHtml(user.ban_reason)}</div>` : ''}

      <div class="reason-field">
        <label for="ban-reason-${user.id}">Moderation note</label>
        <input id="ban-reason-${user.id}" type="text" placeholder="Reason or note for moderation action" value="${escapeHtml(user.ban_reason || '')}">
      </div>

      <div class="action-row">
        ${user.is_authorized_email
          ? `<button class="button button-muted button-compact" type="button" data-action="revoke-user-email" data-email="${escapeHtml(user.email)}">Remove approval</button>`
          : `<button class="button button-success button-compact" type="button" data-action="authorize-user-email" data-email="${escapeHtml(user.email)}">Approve user</button>`}
        ${user.scheduler_enabled
          ? `<button class="button button-warning button-compact" type="button" data-action="stop-user-scheduler" data-user-id="${user.id}">Stop scheduler</button>`
          : ''}
        ${user.is_banned
          ? `<button class="button button-success button-compact" type="button" data-action="unban-user" data-user-id="${user.id}">Unban</button>`
          : `<button class="button button-danger button-compact" type="button" data-action="ban-user" data-user-id="${user.id}">Ban user</button>`}
      </div>
    </article>
  `).join('');
}

function filterEvents(events) {
  if (state.eventFilter === 'all') {
    return events;
  }
  if (state.eventFilter === 'denied') {
    return events.filter(event => String(event.event_type || '').startsWith('auth.login_denied'));
  }
  if (state.eventFilter === 'admin') {
    return events.filter(event => String(event.event_type || '').startsWith('admin.'));
  }
  if (state.eventFilter === 'user') {
    return events.filter(event => String(event.event_type || '').startsWith('user.') || String(event.event_type || '').startsWith('auth.login') || String(event.event_type || '').startsWith('auth.logout'));
  }
  if (state.eventFilter === 'joins') {
    return events.filter(event => String(event.event_type || '') === 'auth.user_created');
  }
  return events;
}

function renderEvents(events) {
  const filtered = filterEvents(events);
  const container = document.getElementById('event-list');
  if (!filtered.length) {
    container.innerHTML = '<div class="empty-state">No audit events match the current filter.</div>';
    return;
  }

  container.innerHTML = filtered.slice(0, 80).map(event => {
    const actor = event.actor_email || event.user_email || 'System';
    const toneClass = String(event.event_type || '').startsWith('auth.login_denied')
      ? 'chip-danger'
      : String(event.event_type || '').startsWith('admin.')
        ? 'chip-warning'
        : 'chip';

    return `
      <article class="event-card">
        <div class="event-card-head">
          <div>
            <div class="tag-row">
              <span class="chip ${toneClass}">${escapeHtml(event.event_type || 'event')}</span>
              ${event.target_email ? `<span class="meta-tag">${escapeHtml(event.target_email)}</span>` : ''}
            </div>
            <div class="event-title">${escapeHtml(event.summary || event.event_type)}</div>
          </div>
          <div class="event-meta">${escapeHtml(formatDateTime(event.created_at))}</div>
        </div>
        <div class="event-body">Actor: ${escapeHtml(actor)}</div>
        ${event.metadata && Object.keys(event.metadata).length
          ? `<div class="event-meta">${escapeHtml(JSON.stringify(event.metadata))}</div>`
          : ''}
      </article>
    `;
  }).join('');
}

function renderDashboard() {
  if (!state.overview) return;
  renderPlatformSnapshot(state.overview.platform || {});
  renderAttentionQueue(state.overview);
  renderSettings(state.overview.settings || {});
  renderAuthorizedEmails(state.overview.authorized_google_emails || []);
  renderUsers(state.overview.users || []);
  renderEvents(state.overview.events || []);
}

function renderOverview(overview) {
  state.overview = overview;
  renderDashboard();
}

async function waitForGoogleIdentity(timeoutMs = 10000) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (window.google?.accounts?.id) {
      return true;
    }
    await new Promise(resolve => window.setTimeout(resolve, 120));
  }
  return false;
}

async function renderGoogleSignInButton(clientId) {
  const container = document.getElementById('admin-google-button');
  if (!container) return;

  if (!clientId) {
    container.innerHTML = '';
    container.classList.remove('is-pending');
    return;
  }

  if (state.session?.google_authenticated) {
    container.innerHTML = '';
    container.classList.remove('is-pending');
    return;
  }

  container.innerHTML = '';
  container.classList.add('is-pending');
  const googleReady = await waitForGoogleIdentity();
  if (!googleReady) {
    setGoogleStatus('Google sign-in did not finish loading. Refresh and try again.', 'error');
    container.classList.remove('is-pending');
    return;
  }

  window.google.accounts.id.initialize({
    client_id: clientId,
    callback: handleGoogleCredentialResponse,
    auto_select: false
  });
  window.google.accounts.id.renderButton(container, {
    theme: 'outline',
    size: 'large',
    shape: 'rectangular',
    text: 'continue_with',
    width: 320
  });
  container.classList.remove('is-pending');
}

async function handleGoogleCredentialResponse(response) {
  if (!response?.credential) {
    showBanner('Google sign-in did not return a usable credential.', 'error');
    return;
  }

  try {
    await apiRequest('/api/auth/google', 'POST', { credential: response.credential });
    await refreshAdminSession({ loadOverviewIfUnlocked: false });
    showBanner('Google sign-in complete. Enter the admin password to continue.', 'success');
  } catch (error) {
    showBanner(error.message, 'error');
    setGoogleStatus(error.message, 'error');
  }
}

async function loadAdminOverview(showSuccessBanner = false) {
  const overview = await apiRequest('/api/admin/overview');
  renderOverview(overview);
  if (showSuccessBanner) {
    showBanner('Admin data refreshed', 'success');
  }
}

async function refreshAdminSession({ loadOverviewIfUnlocked = true } = {}) {
  state.session = await apiRequest('/api/admin/session');
  setSessionUi(state.session);
  await renderGoogleSignInButton(state.session.google_client_id || '');

  if (state.session.authenticated && loadOverviewIfUnlocked) {
    await loadAdminOverview();
  } else if (!state.session.authenticated) {
    document.getElementById('admin-dashboard').hidden = true;
  }

  return state.session;
}

async function initializeAdminPage() {
  try {
    await refreshAdminSession();
  } catch (error) {
    showBanner(error.message, 'error');
  }
}

async function handleAdminLogin(event) {
  event.preventDefault();
  const password = document.getElementById('admin-password').value;

  try {
    state.session = await apiRequest('/api/admin/login', 'POST', { password });
    document.getElementById('admin-password').value = '';
    setSessionUi(state.session);
    await loadAdminOverview();
    showBanner('Admin session unlocked', 'success');
  } catch (error) {
    showBanner(error.message, 'error');
  }
}

async function handleFullSignOut() {
  try {
    await apiRequest('/api/admin/logout', 'POST', {});
  } catch (error) {
  }

  try {
    await apiRequest('/api/auth/logout', 'POST', {});
  } catch (error) {
  }

  if (window.google?.accounts?.id?.disableAutoSelect) {
    window.google.accounts.id.disableAutoSelect();
  }

  state.session = null;
  state.overview = null;
  document.getElementById('admin-password').value = '';
  await refreshAdminSession({ loadOverviewIfUnlocked: false });
  showBanner('Signed out of the admin session', 'success');
}

function webhookTextareaToDestinations() {
  return document.getElementById('admin-webhooks').value
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .map(url => ({ url }));
}

async function handleSettingsSave(event) {
  event.preventDefault();
  try {
    const response = await apiRequest('/api/admin/settings', 'POST', {
      alert_new_users: document.getElementById('alert-new-users').checked,
      alert_user_actions: document.getElementById('alert-user-actions').checked,
      admin_webhook_destinations: webhookTextareaToDestinations()
    });
    renderSettings(response.settings || {});
    await loadAdminOverview();
    showBanner('Platform settings saved', 'success');
  } catch (error) {
    showBanner(error.message, 'error');
  }
}

async function handleTestWebhook() {
  const button = document.getElementById('test-webhook-button');
  if (button) {
    button.disabled = true;
    button.textContent = 'Sending...';
  }

  try {
    const response = await apiRequest('/api/admin/test-webhook', 'POST', {});
    const result = response?.result || {};
    const delivered = Number(result.delivered || 0);
    const attempted = Number(result.attempted || 0);
    showBanner(`Webhook test delivered to ${delivered} of ${attempted} destination${attempted === 1 ? '' : 's'}`, 'success');
  } catch (error) {
    showBanner(error.message, 'error');
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = 'Send Test Alert';
    }
  }
}

async function handleAuthorizedEmailAdd(event) {
  event.preventDefault();
  const emailInput = document.getElementById('authorized-email-input');
  const noteInput = document.getElementById('authorized-email-note');

  try {
    await apiRequest('/api/admin/authorized-emails', 'POST', {
      email: emailInput.value.trim(),
      note: noteInput.value.trim()
    });
    emailInput.value = '';
    noteInput.value = '';
    await loadAdminOverview();
    showBanner('Approved email saved', 'success');
  } catch (error) {
    showBanner(error.message, 'error');
  }
}

async function removeAuthorizedEmail(email) {
  try {
    await apiRequest('/api/admin/authorized-emails/remove', 'POST', { email });
    await loadAdminOverview();
    showBanner('Approved email removed', 'success');
  } catch (error) {
    showBanner(error.message, 'error');
  }
}

async function authorizeUserEmail(email) {
  try {
    await apiRequest('/api/admin/authorized-emails', 'POST', {
      email,
      note: 'Approved from admin panel'
    });
    await loadAdminOverview();
    showBanner('User approved', 'success');
  } catch (error) {
    showBanner(error.message, 'error');
  }
}

async function revokeUserEmail(email) {
  try {
    await apiRequest('/api/admin/authorized-emails/remove', 'POST', { email });
    await loadAdminOverview();
    showBanner('User approval removed', 'success');
  } catch (error) {
    showBanner(error.message, 'error');
  }
}

async function stopUserScheduler(userId) {
  try {
    await apiRequest(`/api/admin/users/${userId}/stop-scheduler`, 'POST', {});
    await loadAdminOverview();
    showBanner('User scheduler stopped', 'success');
  } catch (error) {
    showBanner(error.message, 'error');
  }
}

async function banUser(userId) {
  const reason = document.getElementById(`ban-reason-${userId}`)?.value.trim() || '';
  try {
    await apiRequest(`/api/admin/users/${userId}/ban`, 'POST', { reason });
    await loadAdminOverview();
    showBanner('User banned', 'success');
  } catch (error) {
    showBanner(error.message, 'error');
  }
}

async function unbanUser(userId) {
  try {
    await apiRequest(`/api/admin/users/${userId}/unban`, 'POST', {});
    await loadAdminOverview();
    showBanner('User unbanned', 'success');
  } catch (error) {
    showBanner(error.message, 'error');
  }
}

document.getElementById('admin-login-form').addEventListener('submit', handleAdminLogin);
document.getElementById('admin-settings-form').addEventListener('submit', handleSettingsSave);
document.getElementById('test-webhook-button').addEventListener('click', handleTestWebhook);
document.getElementById('authorized-email-form').addEventListener('submit', handleAuthorizedEmailAdd);
document.getElementById('logout-button').addEventListener('click', handleFullSignOut);
document.getElementById('google-signout-button').addEventListener('click', handleFullSignOut);
document.getElementById('refresh-button').addEventListener('click', () => refreshAdminSession().then(() => {
  if (state.session?.authenticated) {
    showBanner('Admin data refreshed', 'success');
  } else {
    showBanner('Admin session refreshed', 'success');
  }
}).catch(error => {
  showBanner(error.message, 'error');
}));

document.getElementById('user-search-input').addEventListener('input', event => {
  state.userSearch = event.target.value || '';
  if (state.overview) renderUsers(state.overview.users || []);
});

document.getElementById('user-filter-select').addEventListener('change', event => {
  state.userFilter = event.target.value || 'all';
  if (state.overview) renderUsers(state.overview.users || []);
});

document.getElementById('event-filter-select').addEventListener('change', event => {
  state.eventFilter = event.target.value || 'all';
  if (state.overview) renderEvents(state.overview.events || []);
});

document.addEventListener('click', event => {
  const actionElement = event.target instanceof Element ? event.target.closest('[data-action]') : null;
  if (!actionElement) return;

  const action = actionElement.dataset.action;
  if (action === 'remove-authorized-email') {
    removeAuthorizedEmail(actionElement.dataset.email || '');
  } else if (action === 'authorize-user-email') {
    authorizeUserEmail(actionElement.dataset.email || '');
  } else if (action === 'revoke-user-email') {
    revokeUserEmail(actionElement.dataset.email || '');
  } else if (action === 'stop-user-scheduler') {
    stopUserScheduler(Number(actionElement.dataset.userId || 0));
  } else if (action === 'ban-user') {
    banUser(Number(actionElement.dataset.userId || 0));
  } else if (action === 'unban-user') {
    unbanUser(Number(actionElement.dataset.userId || 0));
  }
});

initializeAdminPage();
