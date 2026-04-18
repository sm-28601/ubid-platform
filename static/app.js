// Application State
const state = {
    currentView: 'dashboard',
    stats: null,
    reviewPage: 1,
    searchQuery: '',
};

const API_BASE = '/api';

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    initNavigation();
    initSearch();
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
    
    try {
        const res = await fetch(`${API_BASE}/review/${matchId}/decide`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ decision, notes, reviewer: 'admin' })
        });
        
        if (res.ok) {
            document.getElementById(`match-row-${matchId}`).style.opacity = '0.5';
            document.getElementById(`match-row-${matchId}`).style.pointerEvents = 'none';
            setTimeout(() => {
                loadReviewQueue(state.reviewPage);
                loadDashboard(); // Refresh background stats
            }, 500);
        }
    } catch (err) {
        console.error(err);
        alert('Failed to submit decision');
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
