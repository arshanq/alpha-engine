export default function ProjectDetail({ project, onClose }) {
    if (!project) return null;

    const p = project;
    const successClass = p.success_probability >= 0.3 ? 'high' : p.success_probability >= 0.15 ? 'medium' : 'low';
    const statusClass = p.status.toLowerCase();

    return (
        <div className={`project-detail ${project ? 'project-detail--open' : ''}`}>
            <div className="project-detail__header">
                <div>
                    <div className="project-detail__queue-id">{p.queue_id}</div>
                    <div className="project-detail__title">{p.capacity_mw} MW {p.technology}</div>
                    <div className="project-detail__subtitle">{p.county}, {p.state} • {p.iso}</div>
                </div>
                <button className="project-detail__close" onClick={onClose}>✕</button>
            </div>

            <div className="project-detail__body">
                {/* Status & Score */}
                <div style={{ display: 'flex', gap: 8, marginBottom: 16 }}>
                    <span className={`status-badge status-badge--${statusClass}`}>{p.status}</span>
                    <span className={`score-badge score-badge--${successClass}`}>
                        {(p.success_probability * 100).toFixed(0)}% Success
                    </span>
                </div>

                {p.is_phantom && (
                    <div style={{
                        background: 'rgba(244, 63, 94, 0.1)',
                        border: '1px solid rgba(244, 63, 94, 0.3)',
                        borderRadius: 8,
                        padding: '8px 12px',
                        fontSize: 11,
                        color: '#f43f5e',
                        marginBottom: 16,
                        fontWeight: 500,
                    }}>
                        ⚠️ Flagged as Phantom Load — High withdrawal probability
                    </div>
                )}

                {/* Fields */}
                <div className="project-detail__field">
                    <span className="project-detail__field-label">Developer</span>
                    <span className="project-detail__field-value">{p.developer}</span>
                </div>
                <div className="project-detail__field">
                    <span className="project-detail__field-label">ISO/RTO</span>
                    <span className="project-detail__field-value">{p.iso}</span>
                </div>
                <div className="project-detail__field">
                    <span className="project-detail__field-label">POI</span>
                    <span className="project-detail__field-value">{p.poi_name}</span>
                </div>
                <div className="project-detail__field">
                    <span className="project-detail__field-label">Voltage</span>
                    <span className="project-detail__field-value">{p.voltage_kv} kV</span>
                </div>
                <div className="project-detail__field">
                    <span className="project-detail__field-label">Queue Date</span>
                    <span className="project-detail__field-value">{p.queue_date}</span>
                </div>
                <div className="project-detail__field">
                    <span className="project-detail__field-label">Queue Wait</span>
                    <span className="project-detail__field-value">{Math.round(p.queue_days / 30)} months</span>
                </div>
                <div className="project-detail__field">
                    <span className="project-detail__field-label">Est. COD</span>
                    <span className="project-detail__field-value">{p.estimated_cod}</span>
                </div>
                <div className="project-detail__field">
                    <span className="project-detail__field-label">Capacity</span>
                    <span className="project-detail__field-value">{p.capacity_mw.toLocaleString()} MW</span>
                </div>

                {/* Workforce Estimate */}
                {p.total_workers > 0 && (
                    <div style={{ marginTop: 16 }}>
                        <div style={{
                            fontSize: 10, fontWeight: 600, textTransform: 'uppercase',
                            letterSpacing: '0.5px', color: 'var(--text-muted)', marginBottom: 8,
                        }}>
                            Workforce Estimate
                        </div>
                        <div className="stat-grid">
                            <div className="stat-card">
                                <div className="stat-card__value stat-card__value--amber" style={{ fontSize: 18 }}>
                                    {p.total_workers?.toLocaleString()}
                                </div>
                                <div className="stat-card__label">Total Workers</div>
                            </div>
                            <div className="stat-card">
                                <div className="stat-card__value stat-card__value--cyan" style={{ fontSize: 18 }}>
                                    {p.electricians_needed?.toLocaleString()}
                                </div>
                                <div className="stat-card__label">Electricians</div>
                            </div>
                            <div className="stat-card stat-card--wide">
                                <div className="stat-card__value stat-card__value--violet" style={{ fontSize: 18 }}>
                                    {p.construction_duration_years} years
                                </div>
                                <div className="stat-card__label">Construction Duration</div>
                            </div>
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}
