import { useMemo } from 'react';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, PieChart, Pie } from 'recharts';

/* ── Helpers ── */
function fmtGW(mw) {
    if (mw >= 1000) return `${(mw / 1000).toFixed(1)} GW`;
    return `${Math.round(mw)} MW`;
}

function fmtPct(val) {
    return `${(val * 100).toFixed(1)}%`;
}

const STATUS_COLORS = {
    Operational: '#10b981',
    Active: '#3b82f6',
    Withdrawn: '#f43f5e',
    Suspended: '#f97316',
};

const CustomTooltip = ({ active, payload }) => {
    if (!active || !payload?.length) return null;
    return (
        <div style={{
            background: 'rgba(17, 24, 39, 0.92)',
            border: '1px solid rgba(148, 163, 184, 0.15)',
            borderRadius: 8,
            padding: '8px 12px',
            fontSize: 11,
            color: '#f1f5f9',
            backdropFilter: 'blur(8px)',
        }}>
            <div style={{ fontWeight: 600 }}>{payload[0].payload.name || payload[0].payload.range}</div>
            <div style={{ color: '#94a3b8' }}>{payload[0].value.toLocaleString()} {payload[0].payload.unit || 'projects'}</div>
        </div>
    );
};

export default function Sidebar({
    stats,
    scopedProjects,
    scopeLabel,
    stateSummaries,
    selectedState,
    selectedCounty,
    viewport,
}) {
    // Technology breakdown from scoped projects
    const techBreakdown = useMemo(() => {
        const techs = {};
        scopedProjects.forEach(p => {
            const tech = p.technology || 'Unknown';
            if (!techs[tech]) techs[tech] = { mw: 0, count: 0 };
            techs[tech].mw += p.capacity_mw || 0;
            techs[tech].count += 1;
        });
        return Object.entries(techs)
            .sort(([, a], [, b]) => b.mw - a.mw)
            .slice(0, 8)
            .map(([tech, { mw, count }]) => ({ tech, mw, count }));
    }, [scopedProjects]);

    // Pipeline status breakdown for the donut chart
    const statusBreakdown = useMemo(() => {
        const statuses = {};
        scopedProjects.forEach(p => {
            const s = p.status || 'Unknown';
            if (!statuses[s]) statuses[s] = { count: 0, mw: 0 };
            statuses[s].count += 1;
            statuses[s].mw += p.capacity_mw || 0;
        });
        return Object.entries(statuses)
            .map(([name, { count, mw }]) => ({
                name,
                value: count,
                mw,
                fill: STATUS_COLORS[name] || '#64748b',
            }))
            .sort((a, b) => b.value - a.value);
    }, [scopedProjects]);

    // Score distribution for active projects only
    const SCORE_BUCKETS = [
        { range: '0-10%', min: 0, max: 0.10, color: '#f43f5e' },
        { range: '10-20%', min: 0.10, max: 0.20, color: '#f97316' },
        { range: '20-30%', min: 0.20, max: 0.30, color: '#f59e0b' },
        { range: '30-50%', min: 0.30, max: 0.50, color: '#06b6d4' },
        { range: '50%+', min: 0.50, max: 1.01, color: '#10b981' },
    ];
    const scoreDistribution = useMemo(() => {
        const activeProjects = scopedProjects.filter(p => p.status === 'Active');
        return SCORE_BUCKETS.map(bucket => ({
            ...bucket,
            count: activeProjects.filter(
                p => p.success_probability >= bucket.min && p.success_probability < bucket.max
            ).length,
        }));
    }, [scopedProjects]);

    // Top active projects by capacity
    const topActive = useMemo(() => {
        return scopedProjects
            .filter(p => p.status === 'Active')
            .sort((a, b) => (b.capacity_mw || 0) - (a.capacity_mw || 0))
            .slice(0, 5);
    }, [scopedProjects]);

    // Scope icon
    const scopeIcon = selectedCounty ? '🏙️' : selectedState ? '📍' : '🇺🇸';

    return (
        <>
            {/* ── Scope Indicator ── */}
            <div className="sidebar__section" style={{
                background: selectedState ? 'rgba(59,130,246,0.08)' : 'transparent',
                borderBottom: selectedState ? '2px solid var(--accent-blue)' : '1px solid var(--border-color)',
                paddingBottom: 10,
            }}>
                <div className="sidebar__section-title" style={{
                    color: selectedState ? 'var(--accent-blue)' : 'var(--text-muted)',
                    marginBottom: 8,
                }}>
                    {scopeIcon} Showing: {scopeLabel}
                </div>

                {/* ── 3 KEY INVESTOR METRICS ── */}
                <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: 6 }}>

                    {/* 1. Current Installed Capacity */}
                    <div className="stat-card" style={{ borderLeft: '3px solid #10b981' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
                            <div>
                                <div className="stat-card__label" style={{ fontSize: 9, marginBottom: 2 }}>
                                    OPERATIONAL CAPACITY
                                </div>
                                <div className="stat-card__value stat-card__value--emerald" style={{ fontSize: 22 }}>
                                    {fmtGW(stats.operationalMW)}
                                </div>
                            </div>
                            <div style={{ textAlign: 'right' }}>
                                <div style={{ fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--text-secondary)' }}>
                                    {stats.operationalCount.toLocaleString()} projects
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* 2. Historical Success Rate */}
                    <div className="stat-card" style={{ borderLeft: '3px solid #f59e0b' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
                            <div>
                                <div className="stat-card__label" style={{ fontSize: 9, marginBottom: 2 }}>
                                    HISTORICAL SUCCESS RATE
                                </div>
                                <div className="stat-card__value stat-card__value--amber" style={{ fontSize: 22 }}>
                                    {fmtPct(stats.historicalSuccessRate)}
                                </div>
                            </div>
                            <div style={{ textAlign: 'right', fontSize: 9, color: 'var(--text-muted)', lineHeight: 1.5 }}>
                                <div>by MW capacity</div>
                                <div style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-secondary)', fontSize: 10 }}>
                                    {fmtPct(stats.historicalSuccessRateByCount)} by count
                                </div>
                            </div>
                        </div>
                        <div style={{
                            marginTop: 6, height: 6, borderRadius: 3,
                            background: 'rgba(244,63,94,0.25)', overflow: 'hidden',
                        }}>
                            <div style={{
                                height: '100%', borderRadius: 3,
                                width: `${stats.historicalSuccessRate * 100}%`,
                                background: 'linear-gradient(90deg, #f59e0b, #10b981)',
                                transition: 'width 0.5s ease',
                            }} />
                        </div>
                        <div style={{
                            display: 'flex', justifyContent: 'space-between',
                            fontSize: 9, color: 'var(--text-muted)', marginTop: 3,
                        }}>
                            <span>✅ {fmtGW(stats.operationalMW)} operational</span>
                            <span>❌ {fmtGW(stats.withdrawnMW)} withdrawn</span>
                        </div>
                    </div>

                    {/* 3. Active Pipeline Capacity */}
                    <div className="stat-card" style={{ borderLeft: '3px solid #3b82f6' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
                            <div>
                                <div className="stat-card__label" style={{ fontSize: 9, marginBottom: 2 }}>
                                    ACTIVE PIPELINE
                                </div>
                                <div className="stat-card__value stat-card__value--blue" style={{ fontSize: 22 }}>
                                    {fmtGW(stats.activeMW)}
                                </div>
                            </div>
                            <div style={{ textAlign: 'right' }}>
                                <div style={{ fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--text-secondary)' }}>
                                    {stats.activeCount.toLocaleString()} projects
                                </div>
                                <div style={{ fontSize: 9, color: 'var(--text-muted)' }}>
                                    requesting connection
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {/* ── Pipeline Breakdown (Donut) ── */}
            <div className="sidebar__section">
                <div className="sidebar__section-title">Pipeline Breakdown</div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                    <div style={{ width: 90, height: 90 }}>
                        <ResponsiveContainer width="100%" height="100%">
                            <PieChart>
                                <Pie
                                    data={statusBreakdown}
                                    dataKey="value"
                                    innerRadius={25}
                                    outerRadius={40}
                                    paddingAngle={2}
                                    strokeWidth={0}
                                >
                                    {statusBreakdown.map((entry, idx) => (
                                        <Cell key={idx} fill={entry.fill} fillOpacity={0.85} />
                                    ))}
                                </Pie>
                                <Tooltip content={<CustomTooltip />} />
                            </PieChart>
                        </ResponsiveContainer>
                    </div>
                    <div style={{ flex: 1, fontSize: 10 }}>
                        {statusBreakdown.map(s => (
                            <div key={s.name} style={{
                                display: 'flex', justifyContent: 'space-between',
                                alignItems: 'center', padding: '2px 0',
                            }}>
                                <span style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
                                    <span style={{
                                        width: 8, height: 8, borderRadius: '50%',
                                        background: s.fill, display: 'inline-block',
                                    }} />
                                    <span style={{ color: 'var(--text-secondary)' }}>{s.name}</span>
                                </span>
                                <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-primary)', fontWeight: 500 }}>
                                    {s.value.toLocaleString()} <span style={{ color: 'var(--text-muted)', fontSize: 9 }}>({fmtGW(s.mw)})</span>
                                </span>
                            </div>
                        ))}
                    </div>
                </div>
            </div>

            {/* ── Technology Mix ── */}
            <div className="sidebar__section">
                <div className="sidebar__section-title">Technology Mix</div>
                {techBreakdown.map(({ tech, mw, count }) => (
                    <div key={tech} style={{
                        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                        padding: '3px 0', fontSize: 11,
                    }}>
                        <span style={{ color: 'var(--text-secondary)' }}>{tech}</span>
                        <span style={{
                            fontFamily: 'var(--font-mono)', fontWeight: 500,
                            color: 'var(--text-primary)', fontSize: 11,
                        }}>
                            {fmtGW(mw)} <span style={{ color: 'var(--text-muted)', fontSize: 9 }}>({count})</span>
                        </span>
                    </div>
                ))}
            </div>

            {/* ── Active Pipeline Success Distribution ── */}
            <div className="sidebar__section">
                <div className="sidebar__section-title">Active Projects — Success Forecast</div>
                <div className="chart-container">
                    <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={scoreDistribution} barCategoryGap="20%">
                            <XAxis
                                dataKey="range"
                                tick={{ fontSize: 9, fill: '#64748b' }}
                                axisLine={false}
                                tickLine={false}
                            />
                            <YAxis
                                tick={{ fontSize: 9, fill: '#64748b' }}
                                axisLine={false}
                                tickLine={false}
                                width={25}
                            />
                            <Tooltip content={<CustomTooltip />} cursor={false} />
                            <Bar dataKey="count" radius={[4, 4, 0, 0]}>
                                {scoreDistribution.map((entry, idx) => (
                                    <Cell key={idx} fill={entry.color} fillOpacity={0.75} />
                                ))}
                            </Bar>
                        </BarChart>
                    </ResponsiveContainer>
                </div>
            </div>

            {/* ── Top Active Projects ── */}
            <div className="sidebar__section">
                <div className="sidebar__section-title">Largest Active Requests</div>
                {topActive.length === 0 ? (
                    <div className="empty-state">
                        <div className="empty-state__icon">📋</div>
                        <div>No active projects in scope</div>
                    </div>
                ) : (
                    topActive.map((p, idx) => (
                        <a
                            href={p.project_url || '#'}
                            target="_blank"
                            rel="noreferrer"
                            className="alert-card"
                            key={idx}
                            style={{ display: 'block', textDecoration: 'none', transition: 'background 0.2s', cursor: 'pointer' }}
                            onMouseOver={(e) => e.currentTarget.style.background = 'rgba(255,255,255,0.05)'}
                            onMouseOut={(e) => e.currentTarget.style.background = 'var(--bg-card)'}
                            title={`Open ${p.iso} queue registry for ${p.queue_id}`}
                        >
                            <div className="alert-card__header">
                                <span className="alert-card__location" style={{ fontSize: 10 }}>
                                    {p.queue_id} — {p.technology}
                                </span>
                                <span className="alert-card__mw" style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                                    {fmtGW(p.capacity_mw || 0)}
                                    <span style={{ fontSize: '10px', opacity: 0.5 }}>↗</span>
                                </span>
                            </div>
                            <div className="alert-card__body" style={{ fontSize: 9 }}>
                                <span style={{ color: 'var(--text-muted)' }}>
                                    {p.county ? `${p.county}, ` : ''}{p.state} • {p.poi_name || p.iso}
                                </span>
                                <span style={{
                                    float: 'right',
                                    color: (p.success_probability || 0) > 0.3 ? '#10b981' : '#f59e0b',
                                    fontWeight: 600,
                                }}>
                                    {((p.success_probability || 0) * 100).toFixed(0)}% likely
                                </span>
                            </div>
                        </a>
                    ))
                )}
            </div>
        </>
    );
}
