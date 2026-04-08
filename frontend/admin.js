const runtimeConfig = window.WATCHER_RUNTIME_CONFIG || {};
const apiBase = String(runtimeConfig.apiBaseUrl || window.location.origin).replace(/\/+$/, '');
const statusBanner = document.getElementById('status-banner');

const state = {
  session: null,
  overview: null,
  userSearch: '',
  userFilter: 'all',
  eventFilter: 'all',
  activeReviewKey: ''
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

function formatBytes(value) {
  const numeric = Number(value || 0);
  if (!Number.isFinite(numeric) || numeric <= 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let size = numeric;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  const precision = size >= 100 || unitIndex === 0 ? 0 : size >= 10 ? 1 : 2;
  return `${size.toFixed(precision)} ${units[unitIndex]}`;
}

function formatRate(value) {
  return `${formatBytes(value)}/s`;
}

function formatInterval(value) {
  const minutes = Math.max(1, Number(value || 0));
  if (minutes % 60 === 0) {
    const hours = minutes / 60;
    return hours === 1 ? 'Every 1 hour' : `Every ${hours} hours`;
  }
  return `Every ${minutes} min`;
}

function retailerLabel(retailer) {
  const normalized = String(retailer || 'walgreens').trim().toLowerCase();
  if (normalized === 'cvs') return 'CVS';
  if (normalized === 'bestbuy') return 'Best Buy';
  return 'Walgreens';
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

function setUserFilter(filter) {
  state.userFilter = filter || 'all';
  const select = document.getElementById('user-filter-select');
  if (select) {
    select.value = state.userFilter;
  }
  if (state.overview) {
    renderUsers(state.overview.users || []);
  }
}

function setEventFilter(filter) {
  state.eventFilter = filter || 'all';
  const select = document.getElementById('event-filter-select');
  if (select) {
    select.value = state.eventFilter;
  }
  if (state.overview) {
    renderEvents(state.overview.events || []);
  }
}

function scrollToPanel(panelId) {
  document.getElementById(panelId)?.scrollIntoView({ behavior: 'smooth', block: 'start' });
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
    err.payload = payload;
    throw err;
  }

  return payload;
}

function setGoogleStatus(message, tone = 'info') {
  const element = document.getElementById('admin-google-status');
  element.textContent = message || '';
  element.dataset.tone = tone;
}

function setStateBadge(element, label, state) {
  if (!element) return;
  element.textContent = label;
  element.dataset.state = state;
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
  const googleStepState = document.getElementById('admin-google-step-state');
  const passwordStep = document.getElementById('admin-password-step');
  const passwordCopy = document.getElementById('admin-password-copy');
  const passwordStepState = document.getElementById('admin-password-step-state');
  const passwordStateTitle = document.getElementById('admin-password-state-title');
  const passwordStateCopy = document.getElementById('admin-password-state-copy');
  const passwordHelp = document.getElementById('admin-password-help');
  const passwordInput = document.getElementById('admin-password');
  const passwordSubmit = document.getElementById('admin-password-submit');
  const progressGoogle = document.getElementById('auth-progress-google');
  const progressGoogleStatus = document.getElementById('auth-progress-google-status');
  const progressPassword = document.getElementById('auth-progress-password');
  const progressPasswordStatus = document.getElementById('auth-progress-password-status');
  const progressAccess = document.getElementById('auth-progress-access');
  const progressAccessStatus = document.getElementById('auth-progress-access-status');

  const setProgressState = (element, statusElement, state, label) => {
    if (element) {
      element.dataset.state = state;
    }
    setStateBadge(statusElement, label, state);
  };

  if (!session.configured) {
    authPanel.hidden = false;
    authPanel.dataset.stage = 'unconfigured';
    dashboard.hidden = true;
    logoutButton.hidden = true;
    sessionChip.textContent = 'Admin password missing';
    authCopy.textContent = 'Set ADMIN_PANEL_PASSWORD on the backend before this panel can be used.';
    googleCard.hidden = true;
    googleSignoutButton.hidden = true;
    passwordInput.disabled = true;
    passwordSubmit.disabled = true;
    passwordSubmit.textContent = 'Unavailable';
    passwordInput.placeholder = 'Admin password unavailable';
    passwordCopy.textContent = 'Available only after Google sign-in succeeds.';
    passwordStateTitle.textContent = 'Admin password not configured';
    passwordStateCopy.textContent = 'The backend needs ADMIN_PANEL_PASSWORD before this panel can be unlocked.';
    passwordHelp.textContent = 'Add ADMIN_PANEL_PASSWORD on the server, then refresh this page.';
    passwordStep.dataset.state = 'blocked';
    setStateBadge(googleStepState, 'Blocked', 'blocked');
    setStateBadge(passwordStepState, 'Blocked', 'blocked');
    setProgressState(progressGoogle, progressGoogleStatus, 'blocked', 'Blocked');
    setProgressState(progressPassword, progressPasswordStatus, 'blocked', 'Blocked');
    setProgressState(progressAccess, progressAccessStatus, 'blocked', 'Unavailable');
    setGoogleStatus('Admin password is not configured on the backend.', 'error');
    return;
  }

  const googleAuthenticated = Boolean(session.google_authenticated && session.user);
  const fullyAuthenticated = Boolean(session.authenticated);

  authPanel.hidden = fullyAuthenticated;
  authPanel.dataset.stage = fullyAuthenticated ? 'unlocked' : googleAuthenticated ? 'google-verified' : 'locked';
  dashboard.hidden = !fullyAuthenticated;
  logoutButton.hidden = !googleAuthenticated;

  if (fullyAuthenticated) {
    sessionChip.textContent = `Admin unlocked | ${session.user.email}`;
    authCopy.textContent = 'Admin access is active for this session.';
    passwordCopy.textContent = 'Server-side password accepted for this Google session.';
    passwordStateTitle.textContent = 'Admin access is active';
    passwordStateCopy.textContent = 'This browser session is unlocked. Sign out when you are done moderating.';
    passwordHelp.textContent = 'Admin unlock stays active until you sign out or the session expires.';
    passwordStep.dataset.state = 'complete';
    setStateBadge(googleStepState, 'Verified', 'complete');
    setStateBadge(passwordStepState, 'Accepted', 'complete');
    setProgressState(progressGoogle, progressGoogleStatus, 'complete', 'Verified');
    setProgressState(progressPassword, progressPasswordStatus, 'complete', 'Accepted');
    setProgressState(progressAccess, progressAccessStatus, 'complete', 'Open');
    passwordSubmit.textContent = 'Admin Unlocked';
    passwordInput.placeholder = 'Admin password accepted';
    setGoogleStatus('Google verified and admin password accepted.', 'success');
  } else if (googleAuthenticated) {
    sessionChip.textContent = `Google verified | ${session.user.email}`;
    authCopy.textContent = 'Google sign-in is complete. Enter the admin password to unlock admin access.';
    passwordCopy.textContent = 'Enter the server-side admin password to unlock the admin workspace.';
    passwordStateTitle.textContent = 'Ready for password unlock';
    passwordStateCopy.textContent = 'Google verification is complete. Enter the admin password for this browser session.';
    passwordHelp.textContent = 'Only approved Google accounts can reach this step.';
    passwordStep.dataset.state = 'active';
    setStateBadge(googleStepState, 'Verified', 'complete');
    setStateBadge(passwordStepState, 'Ready', 'active');
    setProgressState(progressGoogle, progressGoogleStatus, 'complete', 'Verified');
    setProgressState(progressPassword, progressPasswordStatus, 'active', 'Ready');
    setProgressState(progressAccess, progressAccessStatus, 'pending', 'Waiting');
    passwordSubmit.textContent = 'Unlock Admin';
    passwordInput.placeholder = 'Enter admin password';
    setGoogleStatus('Google sign-in complete. Continue with the admin password.', 'success');
  } else {
    sessionChip.textContent = 'Locked';
    authCopy.textContent = 'Sign in with an approved Google account first, then enter the server-side admin password to unlock moderation controls.';
    passwordCopy.textContent = 'Available only after Google sign-in succeeds.';
    passwordStateTitle.textContent = 'Waiting on Google sign-in';
    passwordStateCopy.textContent = 'Use an approved Google account first. The password step stays locked until identity verification succeeds.';
    passwordHelp.textContent = 'Google verification must complete before password unlock is available.';
    passwordStep.dataset.state = 'locked';
    setStateBadge(googleStepState, 'Waiting', session.access_denied_reason ? 'error' : 'pending');
    setStateBadge(passwordStepState, 'Locked', 'blocked');
    setProgressState(progressGoogle, progressGoogleStatus, session.access_denied_reason ? 'error' : 'pending', session.access_denied_reason ? 'Denied' : 'Waiting');
    setProgressState(progressPassword, progressPasswordStatus, 'blocked', 'Locked');
    setProgressState(progressAccess, progressAccessStatus, 'blocked', 'Closed');
    passwordSubmit.textContent = 'Sign In First';
    passwordInput.placeholder = 'Google sign-in required first';
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
  passwordSubmit.disabled = !googleAuthenticated || fullyAuthenticated;
}

function renderPlatformSnapshot(platform) {
  const globalStats = platform?.global_statistics || {};
  const uptime = platform?.service_uptime || {};
  const systemStats = platform?.system_stats || {};
  const cpu = systemStats.cpu || {};
  const memory = systemStats.memory || {};
  const disk = systemStats.disk || {};
  const network = systemStats.network || {};
  const totals = platform?.totals || {};

  document.getElementById('hero-uptime-label').textContent = uptime.label || 'Service uptime unavailable';
  document.getElementById('hero-users-total').textContent = `${totals.users || 0} users`;
  document.getElementById('hero-summary-copy').textContent =
    `${totals.scheduler_enabled_users || 0} user schedulers are active, ${totals.login_denials || 0} recent login denials were recorded, ${totals.alert_webhooks || 0} admin alert webhooks are configured, and the VPS is currently at ${formatPercent(cpu.usage_percent || 0)} CPU with ${formatPercent(memory.usage_percent || 0)} memory in use.`;

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

  document.getElementById('platform-system-strip').innerHTML = [
    {
      label: 'CPU',
      value: formatPercent(cpu.usage_percent || 0),
      note: `Load avg ${cpu.load_average?.one_minute ?? 0} / ${cpu.load_average?.five_minutes ?? 0} / ${cpu.load_average?.fifteen_minutes ?? 0}`
    },
    {
      label: 'Memory',
      value: formatPercent(memory.usage_percent || 0),
      note: `${formatBytes(memory.used_bytes || 0)} used of ${formatBytes(memory.total_bytes || 0)}`
    },
    {
      label: 'Storage',
      value: formatPercent(disk.usage_percent || 0),
      note: `${formatBytes(disk.used_bytes || 0)} used of ${formatBytes(disk.total_bytes || 0)}`
    },
    {
      label: 'Network In',
      value: formatRate(network.received_bytes_per_second || 0),
      note: `${formatBytes(network.received_bytes || 0)} received`
    },
    {
      label: 'Network Out',
      value: formatRate(network.transmitted_bytes_per_second || 0),
      note: `${formatBytes(network.transmitted_bytes || 0)} sent`
    }
  ].map(metric => `
    <article class="system-card">
      <div class="metric-label">${escapeHtml(metric.label)}</div>
      <div class="system-value">${escapeHtml(metric.value)}</div>
      <div class="metric-note">${escapeHtml(metric.note)}</div>
    </article>
  `).join('');
}

function buildAttentionItems(overview) {
  const users = overview?.users || [];
  const events = overview?.events || [];
  const totals = overview?.platform?.totals || {};
  const unauthorizedUsers = users.filter(user => !user.is_authorized_email);
  const runningUnauthorized = unauthorizedUsers.filter(user => user.scheduler_enabled);
  const recentlyJoined = users.filter(user => isUserNew(user));
  const deniedEvents = events.filter(event => String(event.event_type || '').startsWith('auth.login_denied'));

  const items = [];

  if (runningUnauthorized.length) {
    items.push({
      key: 'scheduler-review',
      title: `${runningUnauthorized.length} scheduler account${runningUnauthorized.length === 1 ? '' : 's'} need approval review`,
      body: 'These users still have schedulers enabled while their email is waiting for approval.',
      tone: 'warning',
      actionable: true
    });
  }

  if (totals.login_denials) {
    items.push({
      key: 'login-denials',
      title: `${totals.login_denials} recent login denial${totals.login_denials === 1 ? '' : 's'}`,
      body: 'Review the event stream to confirm whether they were expected approval or ban decisions.',
      tone: 'danger',
      actionable: deniedEvents.length > 0
    });
  }

  if (recentlyJoined.length) {
    items.push({
      key: 'new-users',
      title: `${recentlyJoined.length} new user${recentlyJoined.length === 1 ? '' : 's'} in the last 72 hours`,
      body: 'Verify their access posture and decide whether they should remain on the platform.',
      tone: 'success',
      actionable: true
    });
  }

  if (!items.length) {
    items.push({
      key: 'clear',
      title: 'No active operator queue',
      body: 'Nothing urgent is standing out right now. The platform appears stable.',
      tone: 'success',
      actionable: false
    });
  }

  return items;
}

function renderAttentionQueue(overview) {
  const items = buildAttentionItems(overview);
  const container = document.getElementById('attention-list');
  container.innerHTML = items.map(item => `
    <article class="attention-item ${item.actionable ? 'is-actionable' : ''}" ${item.actionable ? 'tabindex="0" role="button"' : ''} data-action="${item.actionable ? 'open-review-item' : ''}" data-review-key="${escapeHtml(item.key || '')}">
      <div class="tag-row">
        <span class="chip ${item.tone === 'danger' ? 'chip-danger' : item.tone === 'warning' ? 'chip-warning' : 'chip-success'}">${escapeHtml(item.tone)}</span>
      </div>
      <strong>${escapeHtml(item.title)}</strong>
      <p>${escapeHtml(item.body)}</p>
    </article>
  `).join('');
}

function closeReviewModal() {
  state.activeReviewKey = '';
  const modal = document.getElementById('review-modal');
  if (modal) {
    modal.hidden = true;
  }
  document.body.style.overflow = '';
}

function renderReviewItems(items = []) {
  const container = document.getElementById('review-modal-list');
  if (!container) return;

  if (!items.length) {
    container.innerHTML = '<div class="empty-state">Nothing actionable is in this queue right now.</div>';
    return;
  }

  container.innerHTML = items.map(item => `
    <article class="review-modal-item">
      <div class="review-modal-item-head">
        <div>
          <strong>${escapeHtml(item.title || 'Item')}</strong>
          ${item.meta ? `<div class="review-modal-item-meta">${escapeHtml(item.meta)}</div>` : ''}
        </div>
        ${item.tag ? `<span class="chip ${item.tagTone || ''}">${escapeHtml(item.tag)}</span>` : ''}
      </div>
      ${item.copy ? `<div class="review-modal-item-copy">${escapeHtml(item.copy)}</div>` : ''}
      ${item.actions?.length ? `<div class="review-modal-item-actions">${item.actions.map(action => `
        <button
          class="button ${action.buttonClass || 'button-muted'} button-compact"
          type="button"
          data-action="${escapeHtml(action.action || '')}"
          ${action.email ? `data-email="${escapeHtml(action.email)}"` : ''}
          ${Number.isFinite(action.userId) ? `data-user-id="${escapeHtml(String(action.userId))}"` : ''}
          ${action.reasonInputId ? `data-reason-input-id="${escapeHtml(action.reasonInputId)}"` : ''}
        >${escapeHtml(action.label || 'Open')}</button>
      `).join('')}</div>` : ''}
    </article>
  `).join('');
}

function getReviewModalConfig(reviewKey) {
  const overview = state.overview || {};
  const users = overview.users || [];
  const events = overview.events || [];
  const unauthorizedUsers = users.filter(user => !user.is_authorized_email);
  const runningUnauthorized = unauthorizedUsers.filter(user => user.scheduler_enabled);
  const recentlyJoined = users.filter(user => isUserNew(user));
  const deniedEvents = events.filter(event => String(event.event_type || '').startsWith('auth.login_denied')).slice(0, 8);

  if (reviewKey === 'scheduler-review') {
    return {
      kicker: 'Pending scheduler review',
      title: 'Approval review for active schedulers',
      copy: 'These accounts are still running scheduled checks while waiting for approval. You can approve them, stop the scheduler, or jump into the filtered users view.',
      items: runningUnauthorized.map(user => ({
        title: user.name || user.email,
        meta: `${user.email} | ${formatInterval(user.check_interval_minutes || 60)} | ZIP ${user.current_zipcode || 'None'}`,
        copy: `${user.tracked_product_count || 0} tracked product${Number(user.tracked_product_count || 0) === 1 ? '' : 's'} | Last login ${formatDateTime(user.last_login_at)}`,
        tag: 'Scheduler on',
        tagTone: 'chip-warning',
        actions: [
          { label: 'Approve user', action: 'authorize-user-email', email: user.email, buttonClass: 'button-success' },
          { label: 'Stop scheduler', action: 'stop-user-scheduler', userId: Number(user.id), buttonClass: 'button-warning' }
        ]
      })),
      footerActions: [
        { label: 'Open pending users', action: 'open-review-filter', target: 'users', filter: 'unauthorized', buttonClass: 'button-primary' }
      ]
    };
  }

  if (reviewKey === 'login-denials') {
    return {
      kicker: 'Access review',
      title: 'Recent login denials',
      copy: 'These recent denials usually mean someone hit the approval gate, a ban, or a bad session state. Open the filtered audit feed to investigate the full timeline.',
      items: deniedEvents.map(event => ({
        title: event.summary || event.event_type || 'Login denial',
        meta: `${formatDateTime(event.created_at)} | ${(event.target_email || event.user_email || 'Unknown user')}`,
        copy: event.metadata && Object.keys(event.metadata).length ? JSON.stringify(event.metadata) : 'No extra metadata recorded.',
        tag: 'Denied',
        tagTone: 'chip-danger',
        actions: []
      })),
      footerActions: [
        { label: 'Open denied events', action: 'open-review-filter', target: 'events', filter: 'denied', buttonClass: 'button-primary' }
      ]
    };
  }

  if (reviewKey === 'new-users') {
    return {
      kicker: 'New users',
      title: 'Recent sign-ups',
      copy: 'Review the newest accounts, approve the ones you want to keep, or open the filtered user list for full moderation details.',
      items: recentlyJoined.map(user => ({
        title: user.name || user.email,
        meta: `${user.email} | Joined ${formatDateTime(user.created_at)}`,
        copy: `${user.is_authorized_email ? 'Already approved' : 'Waiting approval'} | ${user.tracked_product_count || 0} tracked product${Number(user.tracked_product_count || 0) === 1 ? '' : 's'}`,
        tag: user.is_authorized_email ? 'Approved' : 'Pending',
        tagTone: user.is_authorized_email ? 'chip-success' : 'chip-warning',
        actions: user.is_authorized_email
          ? []
          : [{ label: 'Approve user', action: 'authorize-user-email', email: user.email, buttonClass: 'button-success' }]
      })),
      footerActions: [
        { label: 'Open recent users', action: 'open-review-filter', target: 'users', filter: 'new', buttonClass: 'button-primary' }
      ]
    };
  }

  return {
    kicker: 'Review item',
    title: 'Nothing urgent',
    copy: 'The queue is clear right now.',
    items: [],
    footerActions: []
  };
}

function openReviewModal(reviewKey) {
  const config = getReviewModalConfig(reviewKey);
  state.activeReviewKey = reviewKey;

  const modal = document.getElementById('review-modal');
  const kicker = document.getElementById('review-modal-kicker');
  const title = document.getElementById('review-modal-title');
  const copy = document.getElementById('review-modal-copy');
  const actions = document.getElementById('review-modal-actions');

  if (!modal || !kicker || !title || !copy || !actions) return;

  kicker.textContent = config.kicker || 'Review item';
  title.textContent = config.title || 'Needs attention';
  copy.textContent = config.copy || '';
  renderReviewItems(config.items || []);
  actions.innerHTML = (config.footerActions || []).map(action => `
    <button
      class="button ${action.buttonClass || 'button-muted'}"
      type="button"
      data-action="${escapeHtml(action.action || '')}"
      data-review-target="${escapeHtml(action.target || '')}"
      data-review-filter="${escapeHtml(action.filter || '')}"
    >${escapeHtml(action.label || 'Open')}</button>
  `).join('');

  modal.hidden = false;
  document.body.style.overflow = 'hidden';
}

function openReviewFilter(target, filter) {
  closeReviewModal();
  if (target === 'users') {
    setUserFilter(filter || 'all');
    scrollToPanel('users-panel');
  } else if (target === 'events') {
    setEventFilter(filter || 'all');
    scrollToPanel('events-panel');
  }
}

function refreshActiveReviewModal() {
  if (state.activeReviewKey) {
    openReviewModal(state.activeReviewKey);
  }
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

function renderTrendingProductsAdmin(trendingPayload) {
  const container = document.getElementById('trending-admin-list');
  const countBadge = document.getElementById('trending-admin-count');
  const products = Array.isArray(trendingPayload?.products) ? trendingPayload.products : [];
  const retentionHours = Math.max(1, Number(trendingPayload?.retention_hours || 24));

  if (countBadge) {
    countBadge.textContent = `${products.length} product${products.length === 1 ? '' : 's'}`;
  }

  if (!products.length) {
    container.innerHTML = `<div class="empty-state">No trending products are currently visible in the last ${retentionHours} hours.</div>`;
    return;
  }

  container.innerHTML = products.map(product => `
    <article class="trending-admin-item">
      <div class="trending-admin-item-head">
        <div class="trending-admin-copy">
          <strong>${escapeHtml(product.name || product.id || 'Product')}</strong>
          <div class="trending-admin-meta">
            ${escapeHtml(retailerLabel(product.retailer))}
            <span class="meta-divider">|</span>
            ${escapeHtml(String(product.id || 'Unknown ID'))}
            ${product.product_id ? `<span class="meta-divider">|</span>${escapeHtml(product.product_id)}` : ''}
          </div>
        </div>
        <div class="tag-row">
          <span class="chip chip-warning">${escapeHtml(`${Number(product.tracked_by_count || 0)} watcher${Number(product.tracked_by_count || 0) === 1 ? '' : 's'}`)}</span>
        </div>
      </div>
      <div class="trending-admin-item-body">
        <div class="trending-admin-note">Last tracked ${escapeHtml(formatDateTime(product.last_tracked_at))}</div>
        ${product.source_url ? `<a class="button button-muted button-compact" href="${escapeHtml(product.source_url)}" target="_blank" rel="noopener noreferrer">Open link</a>` : ''}
      </div>
      <div class="action-row">
        <button
          class="button button-danger button-compact"
          type="button"
          data-action="delete-trending-product"
          data-product-id="${escapeHtml(product.id || '')}"
          data-retailer="${escapeHtml(product.retailer || 'walgreens')}"
          data-name="${escapeHtml(product.name || product.id || 'Product')}"
        >Delete from trending</button>
      </div>
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
  renderTrendingProductsAdmin(state.overview.trending_products || {});
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
    width: Math.max(240, Math.min(360, Math.floor(container.getBoundingClientRect().width || 320)))
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
    if (!state.session?.authenticated && state.session?.google_authenticated) {
      document.getElementById('admin-password')?.focus();
    }
    showBanner('Google sign-in complete. Enter the admin password to continue.', 'success');
  } catch (error) {
    showBanner(error.message, 'error');
    setGoogleStatus(error.message, 'error');
  }
}

async function loadAdminOverview(showSuccessBanner = false) {
  const overview = await apiRequest('/api/admin/overview');
  renderOverview(overview);
  refreshActiveReviewModal();
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

function formatWebhookTestDetail(result) {
  const failures = (result?.destinations || []).filter(item => !item?.delivered);
  if (!failures.length) {
    return '';
  }

  const firstFailure = failures[0];
  const url = String(firstFailure.url || '');
  let detail = String(firstFailure.error || '').trim();
  if (!detail && firstFailure.status_code) {
    detail = `HTTP ${firstFailure.status_code}`;
  }
  if (!detail) {
    return '';
  }

  try {
    const parsed = new URL(url);
    return `${parsed.hostname}: ${detail}`;
  } catch (error) {
    return detail;
  }
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
  const destinations = webhookTextareaToDestinations();
  if (button) {
    button.disabled = true;
    button.textContent = 'Sending...';
  }

  try {
    if (!destinations.length) {
      throw new Error('Add at least one admin webhook URL before testing.');
    }

    const response = await apiRequest('/api/admin/test-webhook', 'POST', {
      admin_webhook_destinations: destinations
    });
    const result = response?.result || {};
    const delivered = Number(result.delivered || 0);
    const attempted = Number(result.attempted || 0);
    const detail = formatWebhookTestDetail(result);
    const tone = delivered === attempted ? 'success' : 'info';
    const summary = `Webhook test delivered to ${delivered} of ${attempted} destination${attempted === 1 ? '' : 's'}`;
    showBanner(detail ? `${summary}. ${detail}` : summary, tone);
  } catch (error) {
    const result = error?.payload?.result || {};
    const detail = formatWebhookTestDetail(result);
    showBanner(detail ? `${error.message} ${detail}` : error.message, 'error');
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

async function deleteTrendingProduct(actionElement) {
  const productId = String(actionElement?.dataset.productId || '').trim();
  const retailer = String(actionElement?.dataset.retailer || 'walgreens').trim();
  const productName = String(actionElement?.dataset.name || productId).trim() || productId;

  if (!productId) {
    showBanner('Trending product ID missing', 'error');
    return;
  }

  const confirmed = window.confirm(`Delete "${productName}" from the trending products list? This only hides it from Community Radar and does not remove it from user accounts.`);
  if (!confirmed) {
    return;
  }

  const originalLabel = actionElement.textContent;
  actionElement.disabled = true;
  actionElement.textContent = 'Deleting...';

  try {
    await apiRequest('/api/admin/trending-products/remove', 'POST', {
      id: productId,
      retailer,
      name: productName
    });
    await loadAdminOverview();
    showBanner('Trending product removed from admin panel', 'success');
  } catch (error) {
    showBanner(error.message, 'error');
  } finally {
    actionElement.disabled = false;
    actionElement.textContent = originalLabel;
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
  if (action === 'open-review-item') {
    openReviewModal(actionElement.dataset.reviewKey || '');
  } else if (action === 'open-review-filter') {
    openReviewFilter(actionElement.dataset.reviewTarget || '', actionElement.dataset.reviewFilter || '');
  } else if (action === 'close-review-modal') {
    closeReviewModal();
  } else if (action === 'remove-authorized-email') {
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
  } else if (action === 'delete-trending-product') {
    deleteTrendingProduct(actionElement);
  }
});

document.addEventListener('keydown', event => {
  if (event.key === 'Escape' && state.activeReviewKey) {
    closeReviewModal();
    return;
  }

  const actionElement = event.target instanceof Element ? event.target.closest('[data-action="open-review-item"]') : null;
  if (!actionElement) return;
  if (event.key !== 'Enter' && event.key !== ' ') return;
  event.preventDefault();
  openReviewModal(actionElement.dataset.reviewKey || '');
});

initializeAdminPage();
