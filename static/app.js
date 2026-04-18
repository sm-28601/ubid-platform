// Application State
const state = {
    currentView: 'dashboard',
    stats: null,
    reviewPage: 1,
    searchQuery: '',
    language: 'en',
};

const API_BASE = '/api';

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    initNavigation();
    initSearch();
    initLanguage();
    loadDashboard();
});

// Navigation Logic
function initNavigation() {
    const navItems = document.querySelectorAll('.nav-item');
    navItems.forEach(item => {
        item.addEventListener('click', (e) => {
            const view = e.currentTarget.getAttribute('data-view');
            if (view) switchView(view);
        });
    });
}

function switchView(viewName) {
    // Update nav classes
    document.querySelectorAll('.nav-item').forEach(i => i.classList.remove('active'));
    const navBtn = document.getElementById(`nav-${viewName}`);
    if (navBtn) navBtn.classList.add('active');

    // Update views
    document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
    const targetView = document.getElementById(`view-${viewName}`);
    if (targetView) targetView.classList.add('active');

    state.currentView = viewName;

    // Load data based on view
    if (viewName === 'dashboard') loadDashboard();
    if (viewName === 'review') loadReviewQueue();
}

function initLanguage() {
    const select = document.getElementById('language-select');
    if (!select) return;

    select.addEventListener('change', () => {
        applyLanguage(select.value);
    });

    applyLanguage(select.value || 'en');
}

async function applyLanguage(lang) {
    try {
        const res = await fetch(`${API_BASE}/i18n/${lang}`);
        const data = await res.json();
        const strings = data.strings || {};
        state.language = data.lang || lang;

        document.querySelectorAll('[data-i18n]').forEach(node => {
            const key = node.getAttribute('data-i18n');
            if (strings[key]) node.textContent = strings[key];
        });
    } catch (err) {
        console.error('Language load failed', err);
    }
}

// Formatters
const fmtNum = (num) => new Intl.NumberFormat('en-IN').format(num || 0);
const fmtDate = (dateStr) => {
    if (!dateStr) return 'N/A';
    return new Intl.DateTimeFormat('en-IN', { year: 'numeric', month: 'short', day: 'numeric' }).format(new Date(dateStr));
};

// ==========================================
// Dashboard
// ==========================================
async function loadDashboard() {
    try {
        const res = await fetch(`${API_BASE}/dashboard/stats`);
        const data = await res.json();
        
        // Update KPIs
        document.getElementById('kpi-ubids').textContent = fmtNum(data.total_ubids);
        document.getElementById('kpi-ubids-sub').textContent = `${data.dedup_ratio}% deduplication ratio`;
        
        document.getElementById('kpi-records').textContent = fmtNum(data.total_source_records);
        document.getElementById('kpi-records-sub').textContent = `Across ${Object.keys(data.records_by_system || {}).length} systems`;
        
        document.getElementById('kpi-reviews').textContent = fmtNum(data.pending_reviews);
        const badge = document.getElementById('review-badge');
        if(badge) {
            badge.textContent = data.pending_reviews;
            badge.style.display = data.pending_reviews > 0 ? 'inline-block' : 'none';
        }
        
        document.getElementById('kpi-events').textContent = fmtNum(data.total_events);
        document.getElementById('kpi-events-sub').textContent = `${fmtNum(data.matched_events)} matched to UBIDs`;

        document.getElementById('last-updated').textContent = `Last updated: ${new Date().toLocaleTimeString()}`;

        // Render Charts
        renderStatusChart(data.status_breakdown);
        renderDeptChart(data.records_by_system);
        renderDedupChart(data);

    } catch (err) {
        console.error('Failed to load dashboard:', err);
    }
}

function renderStatusChart(statusData) {
    if (!statusData) return;
    const container = document.getElementById('status-chart');
    const total = Object.values(statusData).reduce((a,b)=>a+b, 0);
    
    let html = '';
    for (const [status, count] of Object.entries(statusData)) {
        if(status === 'Merged') continue;
        const pct = total ? (count/total)*100 : 0;
        html += `
            <div class="chart-bar-wrap">
                <div class="chart-label-row">
                    <span class="status-${status} font-bold">${status}</span>
                    <span>${fmtNum(count)} (${pct.toFixed(1)}%)</span>
                </div>
                <div class="chart-bar-bg">
                    <div class="chart-bar-fill bg-${status}" style="width: ${pct}%"></div>
                </div>
            </div>
        `;
    }
    container.innerHTML = html || '<p class="text-muted">No data available.</p>';
}

function renderDeptChart(deptData) {
    if (!deptData) return;
    const container = document.getElementById('dept-chart');
    const total = Object.values(deptData).reduce((a,b)=>a+b, 0);
    
    let html = '';
    // Sort by count
    const sorted = Object.entries(deptData).sort((a,b) => b[1]-a[1]);
    for (const [sys, count] of sorted) {
        const pct = total ? (count/total)*100 : 0;
        html += `
            <div class="chart-bar-wrap">
                <div class="chart-label-row">
                    <span>${sys.toUpperCase()}</span>
                    <span>${fmtNum(count)}</span>
                </div>
                <div class="chart-bar-bg">
                    <div class="chart-bar-fill" style="background:var(--accent-cyan); width: ${pct}%"></div>
                </div>
            </div>
        `;
    }
    container.innerHTML = html || '<p class="text-muted">No data available.</p>';
}

function renderDedupChart(data) {
    const container = document.getElementById('dedup-chart');
    if (!data.total_source_records) return;
    
    const orig = data.total_source_records;
    const ubids = data.total_ubids;
    
    container.innerHTML = `
        <div style="display:flex; justify-content:space-between; margin-bottom:1rem; align-items:center;">
            <div style="text-align:center">
                <div style="font-size:2rem; font-weight:bold">${fmtNum(orig)}</div>
                <div class="text-muted text-sm">Fragmented Records</div>
            </div>
            <div style="color:var(--accent-emerald)">➔</div>
            <div style="text-align:center">
                <div style="font-size:2rem; font-weight:bold; color:var(--accent-emerald)">${fmtNum(ubids)}</div>
                <div class="text-muted text-sm">Unified Entities</div>
            </div>
        </div>
        <p class="text-muted" style="text-align:center; font-size:0.9rem">Reduced overall database size by <strong>${data.dedup_ratio}%</strong></p>
    `;
}

// ==========================================
// Search View
// ==========================================
function initSearch() {
    const input = document.getElementById('search-input');
    input.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') performSearch();
    });
    
    document.querySelectorAll('.hint-chip').forEach(chip => {
        chip.addEventListener('click', () => {
            const val = chip.textContent.split(': ')[1];
            input.value = val;
            performSearch();
        });
    });
}

async function performSearch() {
    const query = document.getElementById('search-input').value.trim();
    if (!query) return;
    
    const container = document.getElementById('search-results');
    container.innerHTML = '<div class="text-center p-4">Searching...</div>';
    
    try {
        const res = await fetch(`${API_BASE}/ubid/search?q=${encodeURIComponent(query)}`);
        const data = await res.json();
        
        if (!data.results || data.results.length === 0) {
            container.innerHTML = `<div class="entity-card"><p class="text-muted">No results found for "${query}"</p></div>`;
            return;
        }
        
        let html = `<h3>Found ${data.results.length} matches (Search Type: ${data.type})</h3>`;
        
        for (const item of data.results) {
            const statusClass = item.activity_status === 'Active' ? 'badge-active' : 
                                item.activity_status === 'Dormant' ? 'badge-dormant' : 'badge-closed';
            
            html += `
                <div class="entity-card">
                    <div class="entity-header">
                        <div>
                            <div class="mb-1 text-sm text-muted font-mono">${item.ubid}</div>
                            <div class="entity-title">${item.canonical_name || 'Unknown Business'}</div>
                        </div>
                        <div class="entity-badge ${statusClass}">${item.activity_status}</div>
                    </div>
                    
                    <div class="text-sm mb-4">${item.canonical_address}, PIN: ${item.pincode}</div>
                    
                    <div class="entity-meta">
                        <div class="meta-item">
                            <span class="meta-label">Anchor</span>
                            <span class="meta-val">${item.anchor_type ? `${item.anchor_type}: ${item.anchor_value}` : 'None'}</span>
                        </div>
                        <div class="meta-item">
                            <span class="meta-label">Linked Systems</span>
                            <span class="tag-list">
                                ${item.systems_present.map(s => `<span class="tag">${s}</span>`).join('')}
                            </span>
                        </div>
                        <div class="meta-item">
                            <span class="meta-label">Activity Events</span>
                            <span class="meta-val">${item.event_count || 0}</span>
                        </div>
                    </div>
                    
                    <div class="mt-4 pt-4" style="border-top:1px solid var(--border-color)">
                        <button class="btn btn-outline" onclick="openUbidModal('${item.ubid}')">View Full Canvas</button>
                        <button class="btn btn-primary" onclick="viewTimelineFromSearch('${item.ubid}')">View Timeline</button>
                    </div>
                </div>
            `;
        }
        
        container.innerHTML = html;
        
    } catch (err) {
        container.innerHTML = `<div class="text-red">Error performing search</div>`;
    }
}

function viewTimelineFromSearch(ubid) {
    document.getElementById('activity-ubid-input').value = ubid;
    switchView('activity');
    loadTimeline();
}

// ==========================================
// Modal
// ==========================================
async function openUbidModal(ubid) {
    const modal = document.getElementById('ubid-modal');
    const body = document.getElementById('modal-body');
    
    body.innerHTML = 'Loading...';
    modal.style.display = 'flex';
    
    try {
        const res = await fetch(`${API_BASE}/ubid/${ubid}`);
        const data = await res.json();
        
        if(data.error) {
            body.innerHTML = `<div class="text-red">${data.error}</div>`;
            return;
        }
        
        let html = `
            <div class="mb-4">
                <h3 class="entity-title">${data.canonical_name}</h3>
                <div class="font-mono text-muted">${data.ubid}</div>
                <div class="mt-2 font-bold status-${data.activity_status}">${data.activity_status}</div>
            </div>
            
            <h4>Linked Source Records (${data.linked_records.length})</h4>
            <div class="mt-4" style="display:flex; flex-direction:column; gap:1rem">
        `;
        
        for (const rec of data.linked_records) {
            html += `
                <div class="entity-card p-4" style="padding:1rem;">
                    <div style="display:flex;justify-content:space-between">
                        <span class="font-bold text-cyan">${rec.source_system.toUpperCase()}</span>
                        <span class="font-mono text-xs">${rec.source_id}</span>
                    </div>
                    <div class="mt-2 text-sm">${rec.raw_name}</div>
                    <div class="text-sm text-muted">${rec.raw_address}</div>
                    <div class="mt-2 tag-list">
                        ${rec.sr_pan ? `<span class="tag">PAN: ${rec.sr_pan}</span>` : ''}
                        ${rec.sr_gstin ? `<span class="tag">GST: ${rec.sr_gstin}</span>` : ''}
                    </div>
                </div>
            `;
        }
        
        html += `</div>`;
        body.innerHTML = html;
    } catch(err) {
        body.innerHTML = 'Error loading details.';
    }
}

function closeModal() {
    document.getElementById('ubid-modal').style.display = 'none';
}

// ==========================================
// Review Queue
// ==========================================
async function loadReviewQueue(page = 1) {
    const container = document.getElementById('review-list');
    container.innerHTML = 'Loading...';
    state.reviewPage = page;
    
    try {
        const res = await fetch(`${API_BASE}/review/pending?page=${page}`);
        const data = await res.json();
        
        document.getElementById('review-counter').textContent = `${data.total} pending`;
        const badge = document.getElementById('review-badge');
        if(badge) { badge.textContent = data.total; badge.style.display = data.total > 0 ? 'inline-block' : 'none'; }
        
        if (data.matches.length === 0) {
            container.innerHTML = '<div class="entity-card p-4 text-center">No pending reviews. Good job!</div>';
            document.getElementById('review-pagination').innerHTML = '';
            return;
        }
        
        let html = '';
        for (const m of data.matches) {
            html += `
                <div class="review-item" id="match-row-${m.id}">
                    <div class="review-header">
                        <div>
                            <div>Candidate Pair #${m.id}</div>
                        </div>
                        <div class="score-badge">${(m.similarity_score * 100).toFixed(1)}% Match Confidence</div>
                    </div>
                    <div class="review-body">
                        <div class="record-panel">
                            <h4>${m.sys_a} | ${m.sid_a}</h4>
                            <div class="font-bold mb-1">${m.name_a}</div>
                            <div class="text-sm mb-2 text-muted">${m.addr_a}, ${m.pin_a}</div>
                            <div class="tag-list">
                                ${m.pan_a ? `<span class="tag">PAN: ${m.pan_a}</span>` : ''}
                                ${m.gstin_a ? `<span class="tag">GST: ${m.gstin_a}</span>` : ''}
                            </div>
                        </div>
                        <div class="record-panel">
                            <h4>${m.sys_b} | ${m.sid_b}</h4>
                            <div class="font-bold mb-1">${m.name_b}</div>
                            <div class="text-sm mb-2 text-muted">${m.addr_b}, ${m.pin_b}</div>
                             <div class="tag-list">
                                ${m.pan_b ? `<span class="tag">PAN: ${m.pan_b}</span>` : ''}
                                ${m.gstin_b ? `<span class="tag">GST: ${m.gstin_b}</span>` : ''}
                            </div>
                        </div>
                    </div>
                    <div class="review-footer">
                        <input type="text" id="notes-${m.id}" placeholder="Optional decision notes...">
                        <button class="btn btn-outline" style="color:var(--accent-red);border-color:var(--accent-red)" onclick="submitDecision(${m.id}, 'reject')">Keep Separate</button>
                        <button class="btn btn-primary" style="background:var(--accent-emerald)" onclick="submitDecision(${m.id}, 'merge')">Merge & Link</button>
                    </div>
                </div>
            `;
        }
        
        container.innerHTML = html;
        
    } catch(err) {
        container.innerHTML = '<div class="text-red">Failed to load review queue.</div>';
    }
}

async function submitDecision(matchId, decision) {
    const notes = document.getElementById(`notes-${matchId}`).value;
    const row = document.getElementById(`match-row-${matchId}`);
    if (row) {
        row.style.opacity = '0.5';
        row.style.pointerEvents = 'none';
    }
    
    try {
        const res = await fetch(`${API_BASE}/review/${matchId}/decide`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ decision, notes, reviewer: 'admin' })
        });
        const payload = await res.json();
        
        if (!res.ok) {
            throw new Error(payload.error || 'Failed to submit decision');
        }

        // Sync visible counters right away, then re-fetch canonical data.
        if (typeof payload.pending_reviews === 'number') {
            const badge = document.getElementById('review-badge');
            const reviewCounter = document.getElementById('review-counter');
            const kpiReviews = document.getElementById('kpi-reviews');

            if (badge) {
                badge.textContent = payload.pending_reviews;
                badge.style.display = payload.pending_reviews > 0 ? 'inline-block' : 'none';
            }
            if (reviewCounter) {
                reviewCounter.textContent = `${payload.pending_reviews} pending`;
            }
            if (kpiReviews) {
                kpiReviews.textContent = fmtNum(payload.pending_reviews);
            }
        }

        await Promise.all([
            loadReviewQueue(state.reviewPage),
            loadDashboard(),
        ]);
    } catch (err) {
        if (row) {
            row.style.opacity = '1';
            row.style.pointerEvents = 'auto';
        }
        console.error(err);
        alert(err.message || 'Failed to submit decision');
    }
}


// ==========================================
// Pipeline Control
// ==========================================
async function runPipeline(step) {
    const statusDiv = document.getElementById(`pipe-status-${step}`);
    const btn = document.getElementById(`btn-${step}`);
    
    statusDiv.textContent = 'Running... please wait.';
    statusDiv.className = 'pipe-status text-amber';
    btn.disabled = true;
    
    try {
        const res = await fetch(`${API_BASE}/pipeline/${step}`, { method: 'POST' });
        const data = await res.json();
        
        statusDiv.className = 'pipe-status text-emerald';
        if (step === 'generate') {
            statusDiv.textContent = `Success: Generated ${data.records_generated} records & ${data.events_generated} events.`;
        } else if (step === 'resolve') {
            statusDiv.textContent = `Success: Clusters formed: ${data.summary.clusters_formed}, Auto-linked: ${data.summary.auto_linked_pairs}, For review: ${data.summary.review_candidates}.`;
        } else if (step === 'activity') {
            statusDiv.textContent = `Success: Evaluated ${data.result.total_evaluated_ubids} businesses. Unmatched events: ${data.result.unmatched_events}.`;
        } else if (step === 'full') {
            statusDiv.textContent = `Success: Pipeline completed end-to-end.`;
        }
        
        // Refresh dashboard background stats
        loadDashboard();
        
    } catch(err) {
        statusDiv.className = 'pipe-status text-red';
        statusDiv.textContent = 'Failed to execute pipeline step.';
        console.error(err);
    } finally {
        btn.disabled = false;
    }
}

// ==========================================
// Activity Timeline
// ==========================================
async function loadTimeline() {
    const ubid = document.getElementById('activity-ubid-input').value.trim();
    if (!ubid) return;
    
    const container = document.getElementById('activity-content');
    container.innerHTML = 'Loading...';
    
    try {
        const res = await fetch(`${API_BASE}/ubid/${ubid}/timeline`);
        const data = await res.json();
        
        if (data.events.length === 0) {
            container.innerHTML = '<p>No activity events found for this UBID.</p>';
            return;
        }
        
        let html = `
            <div class="mb-4 p-4 border rounded" style="border-color:var(--border-color); background:rgba(0,0,0,0.2)">
                <h3 class="mb-2">Current Status: <span class="status-${data.status_info.status}">${data.status_info.status}</span></h3>
                <p class="text-sm text-muted font-mono">Rule triggered: ${data.status_info.evidence?.rule || 'N/A'}</p>
            </div>
            <div class="timeline">
        `;
        
        for (const evt of data.events) {
            html += `
                <div class="timeline-item">
                    <div class="timeline-dot"></div>
                    <div class="timeline-content">
                        <div class="timeline-date">${evt.event_date}</div>
                        <div class="font-bold mb-1">${evt.event_type.toUpperCase().replace(/_/g, ' ')}</div>
                        <div class="text-sm text-cyan mb-2">Source: ${evt.source_system}</div>
                        <div class="text-sm text-muted font-mono bg-main p-2 rounded">${JSON.stringify(evt.event_details || {})}</div>
                    </div>
                </div>
            `;
        }
        
        html += `</div>`;
        container.innerHTML = html;
        
    } catch (err) {
        container.innerHTML = '<div class="text-red">Error loading timeline</div>';
    }
}

// ==========================================
// Analytics Queries
// ==========================================
async function runFeaturedQuery() {
    const pin = document.getElementById('query-pincode').value;
    const months = document.getElementById('query-months').value;
    const container = document.getElementById('featured-query-results');
    
    container.innerHTML = 'Running...';
    try {
        const res = await fetch(`${API_BASE}/query/active-no-inspection?pincode=${pin}&months=${months}`);
        const data = await res.json();
        
        if(data.results.length === 0) {
            container.innerHTML = '<p class="mt-4 text-emerald">No un-inspected active factories found for this criteria.</p>';
            return;
        }
        
        let html = `<p class="mt-4 mb-4 text-amber">Found ${data.total} active factories needing inspection.</p>
        <div style="display:flex;flex-direction:column;gap:1rem;">
        `;
        
        for(const b of data.results) {
            html += `
                <div class="entity-card p-4">
                    <div class="font-mono text-sm">${b.ubid}</div>
                    <div class="font-bold">${b.canonical_name}</div>
                    <div class="text-sm text-muted">${b.canonical_address}</div>
                    <div class="mt-2 text-sm text-red">Last Inspection: ${b.last_inspection}</div>
                </div>
            `;
        }
        html += '</div>';
        container.innerHTML = html;
    } catch(err) {
        container.innerHTML = '<span class="text-red">Query failed</span>';
    }
}

async function runCustomQuery() {
    const status = document.getElementById('cq-status').value;
    const pin = document.getElementById('cq-pincode').value;
    const dept = document.getElementById('cq-department').value;
    const cat = document.getElementById('cq-category').value;
    
    let url = `${API_BASE}/query/custom?`;
    if(status) url += `status=${status}&`;
    if(pin) url += `pincode=${pin}&`;
    if(dept) url += `department=${dept}&`;
    if(cat) url += `category=${cat}`;
    
    const container = document.getElementById('custom-query-results');
    container.innerHTML = 'Running...';
    
    try {
        const res = await fetch(url);
        const data = await res.json();
        
        if (data.results.length === 0) {
            container.innerHTML = '<p class="mt-4 text-muted">No results matched the query.</p>';
            return;
        }
        
        let html = `<p class="mt-4 mb-4 font-bold">Found ${data.total} matches (showing up to 100).</p>
         <div style="display:flex;flex-direction:column;gap:1rem;">`;
         for(const b of data.results) {
            html += `
                <div class="entity-card p-4">
                    <div style="display:flex;justify-content:space-between">
                        <div class="font-mono text-sm text-muted">${b.ubid}</div>
                        <div class="status-${b.activity_status} text-sm font-bold">${b.activity_status}</div>
                    </div>
                    <div class="font-bold mt-1">${b.canonical_name || 'N/A'}</div>
                    <div class="text-sm text-muted">${b.canonical_address || 'N/A'}, ${b.pincode}</div>
                </div>
            `;
         }
         html += '</div>';
         container.innerHTML = html;
    } catch(err) {
        container.innerHTML = '<span class="text-red">Query failed</span>';
    }
}

// ==========================================
// Operations Hub
// ==========================================
async function runUniversalSearchOps() {
    const q = document.getElementById('ops-universal-query').value.trim();
    const out = document.getElementById('ops-universal-results');
    if (!q) return;

    out.innerHTML = 'Searching...';
    try {
        const res = await fetch(`${API_BASE}/search/universal?q=${encodeURIComponent(q)}`);
        const data = await res.json();
        const rows = data.results || [];
        if (rows.length === 0) {
            out.innerHTML = '<p class="text-muted mt-4">No results found.</p>';
            return;
        }

        const html = rows.slice(0, 20).map(r => `
            <div class="entity-card p-4 mt-2">
                <div style="display:flex;justify-content:space-between;gap:1rem;align-items:center">
                    <div>
                        <div class="font-bold">${r.name || r.ubid || r.source_id || 'Record'}</div>
                        <div class="text-sm text-muted">${r.address || ''} ${r.pincode || ''}</div>
                    </div>
                    <div class="tag">${r.entity_type}</div>
                </div>
                <div class="mt-2 text-sm">Score: ${(r.score || 0).toFixed(3)} | Match: ${(r.matched_on || []).join(', ')}</div>
            </div>
        `).join('');

        out.innerHTML = `<p class="mt-4 mb-2">Found ${rows.length} matches.</p>${html}`;
    } catch (err) {
        out.innerHTML = '<p class="text-red mt-4">Search failed.</p>';
    }
}

async function loadEvidenceOps() {
    const ubid = document.getElementById('ops-ubid').value.trim();
    const out = document.getElementById('ops-evidence-results');
    if (!ubid) return;

    out.innerHTML = 'Loading evidence...';
    try {
        const res = await fetch(`${API_BASE}/ubid/${encodeURIComponent(ubid)}/evidence`);
        const data = await res.json();
        if (!res.ok) {
            out.innerHTML = `<p class="text-red">${data.error || 'Failed to load evidence'}</p>`;
            return;
        }

        out.innerHTML = `
            <div class="entity-card p-4 mt-2">
                <div class="font-bold">${data.golden_profile.canonical_name || ubid}</div>
                <div class="text-sm text-muted">${data.golden_profile.canonical_address || ''}</div>
                <div class="mt-2">Linked Records: ${data.source_records.length} | Timeline Events: ${data.activity_timeline.length}</div>
                <div class="mt-2 text-sm text-muted">${data.explanation}</div>
            </div>
        `;
    } catch (err) {
        out.innerHTML = '<p class="text-red">Evidence API failed.</p>';
    }
}

async function recomputeGoldenOps() {
    const ubid = document.getElementById('ops-ubid').value.trim();
    const out = document.getElementById('ops-evidence-results');
    if (!ubid) return;

    out.innerHTML = 'Recomputing golden record...';
    try {
        const res = await fetch(`${API_BASE}/golden/${encodeURIComponent(ubid)}/recompute`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ survivorship_rule: 'latest_verified_address_wins', updated_by: 'admin' }),
        });
        const data = await res.json();
        if (!res.ok) {
            out.innerHTML = `<p class="text-red">${data.error || 'Failed to recompute golden record'}</p>`;
            return;
        }

        const goldenRes = await fetch(`${API_BASE}/golden/${encodeURIComponent(ubid)}`);
        const golden = await goldenRes.json();
        out.innerHTML = `
            <div class="entity-card p-4 mt-2">
                <div class="font-bold">${golden.golden_name || ubid}</div>
                <div class="text-sm text-muted">${golden.golden_address || ''}</div>
                <div class="mt-2 tag-list">
                    ${golden.golden_pan ? `<span class="tag">PAN: ${golden.golden_pan}</span>` : ''}
                    ${golden.golden_gstin ? `<span class="tag">GSTIN: ${golden.golden_gstin}</span>` : ''}
                    ${golden.golden_owner ? `<span class="tag">Owner: ${golden.golden_owner}</span>` : ''}
                </div>
            </div>
        `;
    } catch (err) {
        out.innerHTML = '<p class="text-red">Golden record API failed.</p>';
    }
}

async function loadCockpitOps() {
    const out = document.getElementById('ops-cockpit-results');
    out.innerHTML = 'Loading cockpit...';
    try {
        const res = await fetch(`${API_BASE}/review/cockpit?status=pending&per_page=20`);
        const data = await res.json();
        const items = data.items || [];
        if (items.length === 0) {
            out.innerHTML = '<p class="text-muted mt-4">No pending review cases.</p>';
            return;
        }

        const html = items.map(i => `
            <div class="entity-card p-4 mt-2">
                <div style="display:flex;justify-content:space-between;align-items:center">
                    <div class="font-bold">Case #${i.id}</div>
                    <div class="tag">Priority ${i.priority_score}</div>
                </div>
                <div class="text-sm mt-2">${i.left.name || '-'} ↔ ${i.right.name || '-'}</div>
                <div class="text-sm text-muted">SLA: ${i.sla_age_hours}h | Reason: ${i.priority_reason} | Similarity: ${(i.similarity_score * 100).toFixed(1)}%</div>
            </div>
        `).join('');

        out.innerHTML = `<p class="mt-4 mb-2">Pending: ${data.total}</p>${html}`;
    } catch (err) {
        out.innerHTML = '<p class="text-red">Cockpit API failed.</p>';
    }
}

async function loadAlertsOps() {
    const out = document.getElementById('ops-risk-results');
    out.innerHTML = 'Scanning watchlists...';
    try {
        const res = await fetch(`${API_BASE}/watchlists/alerts?persist=true`);
        const data = await res.json();
        const alerts = data.alerts || [];
        if (alerts.length === 0) {
            out.innerHTML = '<p class="text-muted mt-4">No active alerts.</p>';
            return;
        }
        out.innerHTML = alerts.slice(0, 20).map(a => `
            <div class="entity-card p-4 mt-2">
                <div style="display:flex;justify-content:space-between"><div class="font-bold">${a.title}</div><div class="tag">${a.severity}</div></div>
                <div class="text-sm text-muted mt-1">${a.entity_ref || ''}</div>
                <div class="text-sm mt-1">${a.details || ''}</div>
            </div>
        `).join('');
    } catch (err) {
        out.innerHTML = '<p class="text-red">Watchlist scan failed.</p>';
    }
}

async function loadDQOps() {
    const out = document.getElementById('ops-risk-results');
    out.innerHTML = 'Loading data quality...';
    try {
        const res = await fetch(`${API_BASE}/data-quality/summary`);
        const data = await res.json();
        out.innerHTML = `
            <div class="entity-card p-4 mt-2">
                <div class="font-bold">Data Quality Summary</div>
                <div class="mt-2 text-sm">Total Records: ${fmtNum(data.total_records)}</div>
                <div class="text-sm">Missing PAN: ${fmtNum(data.missing_pan)}</div>
                <div class="text-sm">Missing GSTIN: ${fmtNum(data.missing_gstin)}</div>
                <div class="text-sm">Incomplete Address: ${fmtNum(data.incomplete_address)}</div>
                <div class="text-sm">Duplicate Groups: ${fmtNum(data.duplicate_groups_within_department)}</div>
            </div>
        `;
    } catch (err) {
        out.innerHTML = '<p class="text-red">Data quality API failed.</p>';
    }
}

async function loadGeoOps() {
    const out = document.getElementById('ops-risk-results');
    out.innerHTML = 'Loading geo analytics...';
    try {
        const res = await fetch(`${API_BASE}/geo/analytics`);
        const data = await res.json();
        const clusters = data.clusters || [];
        out.innerHTML = clusters.slice(0, 20).map(c => `
            <div class="entity-card p-4 mt-2">
                <div class="font-bold">Pincode ${c.pincode}</div>
                <div class="text-sm">Businesses: ${fmtNum(c.total)}</div>
                <div class="text-sm text-muted">${JSON.stringify(c.status_breakdown || {})}</div>
            </div>
        `).join('');
    } catch (err) {
        out.innerHTML = '<p class="text-red">Geo analytics API failed.</p>';
    }
}

async function simulatePolicyOps() {
    const out = document.getElementById('ops-risk-results');
    out.innerHTML = 'Running policy simulation...';
    try {
        const res = await fetch(`${API_BASE}/policy/simulate/dormant-threshold?from_months=9&to_months=12`);
        const data = await res.json();
        out.innerHTML = `
            <div class="entity-card p-4 mt-2">
                <div class="font-bold">Policy Simulation</div>
                <div class="text-sm mt-2">From ${data.from_months} months to ${data.to_months} months dormant threshold</div>
                <div class="text-sm">Entities with changed status: ${fmtNum(data.moved_count)}</div>
            </div>
        `;
    } catch (err) {
        out.innerHTML = '<p class="text-red">Policy simulation API failed.</p>';
    }
}

async function runNetworkGraphOps() {
    const out = document.getElementById('ops-intelligence-results');
    out.innerHTML = 'Building graph...';
    try {
        const res = await fetch(`${API_BASE}/graph/network?min_cluster=3`);
        const data = await res.json();
        const s = data.summary || {};
        const clusters = data.suspicious_clusters || [];
        const shells = data.shell_patterns || [];

        out.innerHTML = `
            <div class="entity-card p-4 mt-2">
                <div class="font-bold">Network Graph Summary</div>
                <div class="text-sm mt-2">Business Nodes: ${fmtNum(s.business_nodes || 0)}</div>
                <div class="text-sm">Entity Nodes: ${fmtNum(s.entity_nodes || 0)}</div>
                <div class="text-sm">Edges: ${fmtNum(s.edges || 0)}</div>
                <div class="text-sm">Suspicious Clusters: ${fmtNum(s.suspicious_cluster_count || 0)}</div>
                <div class="text-sm">Potential Shell Patterns: ${fmtNum(s.shell_pattern_count || 0)}</div>
            </div>
            ${clusters.slice(0, 6).map(c => `
                <div class="entity-card p-4 mt-2">
                    <div style="display:flex;justify-content:space-between"><div class="font-bold">${c.pattern}</div><div class="tag">${c.severity}</div></div>
                    <div class="text-sm mt-1">Entity: ${c.entity_id}</div>
                    <div class="text-sm text-muted">Linked businesses: ${fmtNum(c.degree)}</div>
                </div>
            `).join('')}
            ${shells.slice(0, 4).map(p => `
                <div class="entity-card p-4 mt-2">
                    <div class="font-bold">Potential shell pair</div>
                    <div class="text-sm mt-1">${p.business_a} ↔ ${p.business_b}</div>
                    <div class="text-sm text-muted">Shared links: ${p.shared_count}</div>
                </div>
            `).join('')}
        `;
    } catch (err) {
        out.innerHTML = '<p class="text-red">Network graph API failed.</p>';
    }
}

async function runLearningPreviewOps() {
    const out = document.getElementById('ops-intelligence-results');
    out.innerHTML = 'Loading learning status...';
    try {
        const statusRes = await fetch(`${API_BASE}/learning/status`);
        const status = await statusRes.json();
        const previewRes = await fetch(`${API_BASE}/learning/process`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ apply: false }),
        });
        const preview = await previewRes.json();

        out.innerHTML = `
            <div class="entity-card p-4 mt-2">
                <div class="font-bold">Continuous Learning (Human Controlled)</div>
                <div class="text-sm mt-2">Total Feedback: ${fmtNum(status.total_feedback || 0)}</div>
                <div class="text-sm">Processed Feedback: ${fmtNum(status.processed_feedback || 0)}</div>
                <div class="text-sm">Pending Feedback: ${fmtNum(preview.pending_feedback || status.pending_feedback || 0)}</div>
                <div class="text-sm text-muted mt-2">${status.note || ''}</div>
            </div>
        `;
    } catch (err) {
        out.innerHTML = '<p class="text-red">Learning preview failed.</p>';
    }
}

async function runLearningApplyOps() {
    const out = document.getElementById('ops-intelligence-results');
    out.innerHTML = 'Applying learning updates...';
    try {
        const res = await fetch(`${API_BASE}/learning/process`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ apply: true }),
        });
        const data = await res.json();

        out.innerHTML = `
            <div class="entity-card p-4 mt-2">
                <div class="font-bold">Learning Applied</div>
                <div class="text-sm mt-2">Processed items: ${fmtNum(data.processed_items || 0)}</div>
                <div class="text-sm text-muted">${data.note || ''}</div>
            </div>
        `;
    } catch (err) {
        out.innerHTML = '<p class="text-red">Learning apply failed.</p>';
    }
}

async function runDeptScorecardsOps() {
    const out = document.getElementById('ops-intelligence-results');
    out.innerHTML = 'Loading department scorecards...';
    try {
        const res = await fetch(`${API_BASE}/department/scorecards`);
        const data = await res.json();
        const cards = data.scorecards || [];
        if (cards.length === 0) {
            out.innerHTML = '<p class="text-muted mt-4">No department scorecards available.</p>';
            return;
        }

        out.innerHTML = cards.slice(0, 10).map(c => `
            <div class="entity-card p-4 mt-2">
                <div style="display:flex;justify-content:space-between"><div class="font-bold">${c.source_system}</div><div class="tag">Score ${c.overall_score}</div></div>
                <div class="text-sm mt-1">Feed freshness (days): ${c.feed_freshness_days ?? 'N/A'}</div>
                <div class="text-sm">Unresolved backlog: ${fmtNum(c.unresolved_backlog || 0)}</div>
                <div class="text-sm text-muted">Top DQ: ${(c.top_data_quality_issues || []).map(i => `${i.issue} ${i.percent}%`).join(' | ')}</div>
            </div>
        `).join('');
    } catch (err) {
        out.innerHTML = '<p class="text-red">Department scorecards failed.</p>';
    }
}

async function runExecutiveDashboardOps() {
    const out = document.getElementById('ops-intelligence-results');
    out.innerHTML = 'Loading executive dashboard...';
    try {
        const res = await fetch(`${API_BASE}/executive/dashboard`);
        const data = await res.json();
        const growth = data.sector_growth || [];
        const density = data.active_business_density || [];
        const heat = data.compliance_gap_heatmap || [];

        out.innerHTML = `
            <div class="entity-card p-4 mt-2">
                <div class="font-bold">Executive Summary</div>
                <div class="text-sm mt-2">Active businesses: ${fmtNum(data.summary?.active_business_count || 0)}</div>
                <div class="text-sm">Total businesses: ${fmtNum(data.summary?.total_business_count || 0)}</div>
                <div class="text-sm">Watchlist alerts: ${fmtNum(data.summary?.watchlist_alert_count || 0)}</div>
            </div>
            <div class="entity-card p-4 mt-2">
                <div class="font-bold">Top Sector Growth</div>
                ${(growth.slice(0, 5).map(g => `<div class="text-sm mt-1">${g.sector}: ${g.growth_percent}%</div>`).join('')) || '<div class="text-sm mt-1 text-muted">No data</div>'}
            </div>
            <div class="entity-card p-4 mt-2">
                <div class="font-bold">Active Business Density</div>
                ${(density.slice(0, 5).map(d => `<div class="text-sm mt-1">${d.pincode}: ${d.active_density_percent}%</div>`).join('')) || '<div class="text-sm mt-1 text-muted">No data</div>'}
            </div>
            <div class="entity-card p-4 mt-2">
                <div class="font-bold">Compliance Gap Heatmap (Top)</div>
                ${(heat.slice(0, 5).map(h => `<div class="text-sm mt-1">${h.pincode}: ${fmtNum(h.issues)} issues</div>`).join('')) || '<div class="text-sm mt-1 text-muted">No data</div>'}
            </div>
        `;
    } catch (err) {
        out.innerHTML = '<p class="text-red">Executive dashboard API failed.</p>';
    }
}

async function loadTrustScoreOps() {
    const ubid = document.getElementById('ops-trust-ubid').value.trim();
    const out = document.getElementById('ops-workflow-results');
    if (!ubid) {
        out.innerHTML = '<p class="text-muted mt-4">Enter a UBID first.</p>';
        return;
    }

    out.innerHTML = 'Loading trust score...';
    try {
        const res = await fetch(`${API_BASE}/trust-score/${encodeURIComponent(ubid)}`);
        const data = await res.json();
        if (!res.ok) {
            out.innerHTML = `<p class="text-red">${data.error || 'Trust score failed'}</p>`;
            return;
        }

        out.innerHTML = `
            <div class="entity-card p-4 mt-2">
                <div style="display:flex;justify-content:space-between"><div class="font-bold">Trust Score: ${data.trust_score}</div><div class="tag">${data.status}</div></div>
                <div class="text-sm mt-2">Identity confidence: ${data.components.identity_confidence}</div>
                <div class="text-sm">Activity health: ${data.components.activity_health}</div>
                <div class="text-sm">Compliance health: ${data.components.compliance_health}</div>
                <div class="text-sm">Network risk: ${data.components.network_risk}</div>
                <div class="text-sm text-muted mt-2">${(data.explanation || []).join(' ')}</div>
            </div>
        `;
    } catch (err) {
        out.innerHTML = '<p class="text-red">Trust score API failed.</p>';
    }
}

async function loadInspectionPriorityOps() {
    const out = document.getElementById('ops-workflow-results');
    out.innerHTML = 'Loading inspection workflow...';
    try {
        const res = await fetch(`${API_BASE}/workflows/inspection-priority?limit=20&months_without_inspection=18`);
        const data = await res.json();
        const items = data.items || [];
        if (items.length === 0) {
            out.innerHTML = '<p class="text-muted mt-4">No pending inspection-priority items.</p>';
            return;
        }

        out.innerHTML = items.map(i => `
            <div class="entity-card p-4 mt-2">
                <div style="display:flex;justify-content:space-between"><div class="font-bold">${i.business_name || i.ubid}</div><div class="tag">Priority ${i.priority_score}</div></div>
                <div class="text-sm mt-1">${i.ubid} | PIN ${i.pincode || '-'}</div>
                <div class="text-sm">Trust: ${i.trust_score} | Overdue days: ${i.days_overdue_vs_policy}</div>
                <div class="text-sm text-muted">${i.reason}</div>
            </div>
        `).join('');
    } catch (err) {
        out.innerHTML = '<p class="text-red">Inspection workflow API failed.</p>';
    }
}

async function loadRenewalRiskOps() {
    const out = document.getElementById('ops-workflow-results');
    out.innerHTML = 'Loading renewal-risk workflow...';
    try {
        const res = await fetch(`${API_BASE}/workflows/renewal-risk?limit=20`);
        const data = await res.json();
        const items = data.items || [];
        if (items.length === 0) {
            out.innerHTML = '<p class="text-muted mt-4">No renewal-risk items detected.</p>';
            return;
        }

        out.innerHTML = items.map(i => `
            <div class="entity-card p-4 mt-2">
                <div style="display:flex;justify-content:space-between"><div class="font-bold">${i.business_name || i.ubid}</div><div class="tag">Risk ${i.priority_score}</div></div>
                <div class="text-sm mt-1">${i.ubid}</div>
                <div class="text-sm">Last renewal: ${i.last_renewal_date || 'N/A'} | Recent signals: ${i.recent_signal_count}</div>
                <div class="text-sm text-muted">${i.reason}</div>
            </div>
        `).join('');
    } catch (err) {
        out.innerHTML = '<p class="text-red">Renewal-risk workflow API failed.</p>';
    }
}

async function loadShellBundlesOps() {
    const out = document.getElementById('ops-workflow-results');
    out.innerHTML = 'Loading shell-review bundles...';
    try {
        const res = await fetch(`${API_BASE}/workflows/shell-review-bundles?limit=20`);
        const data = await res.json();
        const items = data.items || [];
        if (items.length === 0) {
            out.innerHTML = '<p class="text-muted mt-4">No shell-review bundles found.</p>';
            return;
        }

        out.innerHTML = items.map(i => `
            <div class="entity-card p-4 mt-2">
                <div style="display:flex;justify-content:space-between"><div class="font-bold">${i.bundle_id}</div><div class="tag">Suspicion ${i.suspicion_score}</div></div>
                <div class="text-sm mt-1">${i.business_a} ↔ ${i.business_b}</div>
                <div class="text-sm">Shared links: ${i.shared_count}</div>
                <div class="text-sm text-muted">${(i.shared_entities || []).join(' | ')}</div>
            </div>
        `).join('');
    } catch (err) {
        out.innerHTML = '<p class="text-red">Shell-review workflow API failed.</p>';
    }
}
