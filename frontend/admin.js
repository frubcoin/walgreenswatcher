const runtimeConfig = window.WATCHER_RUNTIME_CONFIG || {};
const apiBase = String(runtimeConfig.apiBaseUrl || window.location.origin).replace(/\/+$/, '');
const statusBanner = document.getElementById('status-banner');

let latestOverview = null;
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
  const passwordAuthenticated = Boolean(session.password_authenticated);
  const fullyAuthenticated = Boolean(session.authenticated);

  authPanel.hidden = fullyAuthenticated;
  dashboard.hidden = !fullyAuthenticated;
  logoutButton.hidden = !googleAuthenticated && !passwordAuthenticated;

  if (fullyAuthenticated) {
    sessionChip.textContent = `Admin unlocked | ${session.user.email}`;
    authCopy.textContent = 'Admin access is active for this session.';
    setGoogleStatus('Google verified and admin password accepted.', 'success');
  } else if (googleAuthenticated) {
    sessionChip.textContent = `Google verified | ${session.user.email}`;
    authCopy.textContent = 'Google sign-in is complete. Enter the admin password to unlock the panel.';
    setGoogleStatus('Google sign-in complete. Continue with the admin password.', 'success');
  } else {
    sessionChip.textContent = 'Locked';
    authCopy.textContent = 'Sign in with an authorized Google account first, then enter the server-side admin password to unlock moderation controls.';
    if (session.access_denied_reason) {
      setGoogleStatus(session.access_denied_reason, 'error');
    } else if (!session.google_client_id) {
      setGoogleStatus('GOOGLE_CLIENT_ID is not configured on the backend.', 'error');
    } else {
      setGoogleStatus('Use one of the authorized Google emails for this app.', 'info');
    }
  }

  if (googleAuthenticated) {
    googleCard.hidden = false;
    googleName.textContent = session.user.name || session.user.email;
    googleEmail.textContent = session.user.email || '';
    googleCopy.textContent = 'Your current Google session will be checked against the app allowlist.';
  } else {
    googleCard.hidden = true;
    googleName.textContent = '';
    googleEmail.textContent = '';
    googleCopy.textContent = 'Use one of the authorized Google emails for this app.';
  }

  googleSignoutButton.hidden = !googleAuthenticated;
  passwordInput.disabled = !googleAuthenticated;
  passwordSubmit.disabled = !googleAuthenticated;
}

function renderStats(overview) {
  const users = overview.users || [];
  const authorizedEmails = overview.authorized_google_emails || [];
  const settings = overview.settings || {};
  const bannedUsers = users.filter(user => user.is_banned);
  const webhooks = settings.admin_webhook_destinations || [];

  document.getElementById('stat-users').textContent = String(users.length);
  document.getElementById('stat-users-meta').textContent = `${users.filter(user => user.scheduler_enabled).length} running schedulers`;
  document.getElementById('stat-allowlist').textContent = String(authorizedEmails.length);
  document.getElementById('stat-allowlist-meta').textContent = settings.google_allowlist_enabled ? 'Allowlist enforced' : 'Allowlist optional';
  document.getElementById('stat-banned').textContent = String(bannedUsers.length);
  document.getElementById('stat-banned-meta').textContent = bannedUsers.length ? 'Accounts blocked' : 'No current bans';
  document.getElementById('stat-webhooks').textContent = String(webhooks.length);
  document.getElementById('stat-webhooks-meta').textContent = `${overview.events?.length || 0} recent audit events`;
}

function renderSettings(settings) {
  document.getElementById('allowlist-enabled').checked = Boolean(settings.google_allowlist_enabled);
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
    container.innerHTML = '<div class="empty-state">No authorized Google emails have been added yet.</div>';
    return;
  }

  container.innerHTML = entries.map(entry => `
    <article class="token-row">
      <div class="token-main">
        <strong>${escapeHtml(entry.email)}</strong>
        <span>${escapeHtml(entry.note || 'No note')} | Added ${escapeHtml(formatDateTime(entry.added_at))}</span>
      </div>
      <button class="danger-button" type="button" data-action="remove-authorized-email" data-email="${escapeHtml(entry.email)}">Remove</button>
    </article>
  `).join('');
}

function renderUsers(users) {
  const container = document.getElementById('user-list');
  if (!users.length) {
    container.innerHTML = '<div class="empty-state">No user accounts have signed in yet.</div>';
    return;
  }

  container.innerHTML = users.map(user => `
    <article class="user-card ${user.is_banned ? 'is-banned' : ''}">
      <div class="user-card-header">
        <div>
          <strong class="user-card-title">${escapeHtml(user.name || user.email)}</strong>
          <div class="user-meta">${escapeHtml(user.email)} | Joined ${escapeHtml(formatDateTime(user.created_at))}</div>
        </div>
        <div class="user-badges">
          <span class="badge ${user.is_banned ? 'badge-danger' : 'badge-success'}">${user.is_banned ? 'Banned' : 'Active'}</span>
          <span class="badge ${user.is_authorized_email ? 'badge-success' : ''}">${user.is_authorized_email ? 'Authorized Email' : 'Not Allowlisted'}</span>
          <span class="badge">${user.scheduler_enabled ? 'Scheduler On' : 'Scheduler Off'}</span>
        </div>
      </div>
      <div class="user-grid">
        <div class="user-metric">
          <strong>${escapeHtml(String(user.tracked_product_count || 0))}</strong>
          <span>Tracked products</span>
        </div>
        <div class="user-metric">
          <strong>${escapeHtml(String(user.total_checks || 0))}</strong>
          <span>Total checks</span>
        </div>
        <div class="user-metric">
          <strong>${escapeHtml(user.current_zipcode || 'None')}</strong>
          <span>ZIP code</span>
        </div>
        <div class="user-metric">
          <strong>${escapeHtml(formatDateTime(user.last_login_at))}</strong>
          <span>Last login</span>
        </div>
      </div>
      ${user.ban_reason ? `<div class="user-meta">Ban reason: ${escapeHtml(user.ban_reason)}</div>` : ''}
      <div class="user-card-actions">
        <input class="reason-input" id="ban-reason-${user.id}" type="text" placeholder="Ban reason (optional)" value="${escapeHtml(user.ban_reason || '')}">
        ${user.is_banned
          ? `<button class="primary-button" type="button" data-action="unban-user" data-user-id="${user.id}">Unban</button>`
          : `<button class="danger-button" type="button" data-action="ban-user" data-user-id="${user.id}">Ban User</button>`}
      </div>
    </article>
  `).join('');
}

function renderEvents(events) {
  const container = document.getElementById('event-list');
  if (!events.length) {
    container.innerHTML = '<div class="empty-state">No audit events recorded yet.</div>';
    return;
  }

  container.innerHTML = events.map(event => {
    const actor = event.actor_email || event.user_email || 'System';
    const target = event.target_email ? `Target: ${escapeHtml(event.target_email)}` : '';
    const metadata = event.metadata && Object.keys(event.metadata).length
      ? `<div class="event-meta">${escapeHtml(JSON.stringify(event.metadata))}</div>`
      : '';

    return `
      <article class="event-card">
        <div class="event-card-header">
          <strong class="event-card-title">${escapeHtml(event.summary || event.event_type)}</strong>
          <span class="event-meta">${escapeHtml(formatDateTime(event.created_at))}</span>
        </div>
        <div class="event-body">${escapeHtml(event.event_type)} | Actor: ${escapeHtml(actor)}</div>
        ${target ? `<div class="event-meta">${target}</div>` : ''}
        ${metadata}
      </article>
    `;
  }).join('');
}

function renderOverview(overview) {
  latestOverview = {
    ...overview,
    sessionState: latestOverview?.sessionState || null
  };
  renderStats(overview);
  renderSettings(overview.settings || {});
  renderAuthorizedEmails(overview.authorized_google_emails || []);
  renderUsers(overview.users || []);
  renderEvents(overview.events || []);
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

async function renderGoogleSignInButton(clientId, { force = false } = {}) {
  const container = document.getElementById('admin-google-button');
  if (!container) return;

  const session = latestOverview?.sessionState || null;
  if (!clientId) {
    container.innerHTML = '';
    container.classList.remove('is-pending');
    return;
  }

  if (session?.google_authenticated && !force) {
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
  const session = await apiRequest('/api/admin/session');
  latestOverview = { ...(latestOverview || {}), sessionState: session };
  setSessionUi(session);
  await renderGoogleSignInButton(session.google_client_id || '');

  if (session.authenticated && loadOverviewIfUnlocked) {
    await loadAdminOverview();
  } else if (!session.authenticated) {
    document.getElementById('admin-dashboard').hidden = true;
  }

  return session;
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
    const session = await apiRequest('/api/admin/login', 'POST', { password });
    document.getElementById('admin-password').value = '';
    latestOverview = { ...(latestOverview || {}), sessionState: session };
    setSessionUi(session);
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

  latestOverview = null;
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
      google_allowlist_enabled: document.getElementById('allowlist-enabled').checked,
      alert_new_users: document.getElementById('alert-new-users').checked,
      alert_user_actions: document.getElementById('alert-user-actions').checked,
      admin_webhook_destinations: webhookTextareaToDestinations()
    });
    renderSettings(response.settings || {});
    await loadAdminOverview();
    showBanner('Admin settings saved', 'success');
  } catch (error) {
    showBanner(error.message, 'error');
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
    showBanner('Authorized email saved', 'success');
  } catch (error) {
    showBanner(error.message, 'error');
  }
}

async function removeAuthorizedEmail(email) {
  try {
    await apiRequest('/api/admin/authorized-emails/remove', 'POST', { email });
    await loadAdminOverview();
    showBanner('Authorized email removed', 'success');
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
document.getElementById('authorized-email-form').addEventListener('submit', handleAuthorizedEmailAdd);
document.getElementById('logout-button').addEventListener('click', handleFullSignOut);
document.getElementById('google-signout-button').addEventListener('click', handleFullSignOut);
document.getElementById('refresh-button').addEventListener('click', () => refreshAdminSession().then(() => {
  if (latestOverview?.sessionState?.authenticated) {
    showBanner('Admin data refreshed', 'success');
  } else {
    showBanner('Admin session refreshed', 'success');
  }
}).catch(error => {
  showBanner(error.message, 'error');
}));

document.addEventListener('click', event => {
  const actionElement = event.target instanceof Element ? event.target.closest('[data-action]') : null;
  if (!actionElement) return;

  const action = actionElement.dataset.action;
  if (action === 'remove-authorized-email') {
    removeAuthorizedEmail(actionElement.dataset.email || '');
  }
  if (action === 'ban-user') {
    banUser(Number(actionElement.dataset.userId || 0));
  }
  if (action === 'unban-user') {
    unbanUser(Number(actionElement.dataset.userId || 0));
  }
});

initializeAdminPage();
