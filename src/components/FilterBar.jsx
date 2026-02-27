export default function FilterBar({
    isos, activeISOs, toggleISO,
    technologies, activeTechs, toggleTech,
    statuses, activeStatuses, toggleStatus,
    ages, activeAges, toggleAge,
    hidePhantom, setHidePhantom,
}) {
    return (
        <div className="filter-bar">
            {/* Bullshit Filter */}
            <div className="filter-section">
                <div
                    className={`filter-toggle ${hidePhantom ? 'filter-toggle--active' : ''}`}
                    onClick={() => setHidePhantom(!hidePhantom)}
                    style={{ marginBottom: 0 }}
                >
                    <div className="filter-toggle__switch" />
                    <span className="filter-toggle__label">
                        {hidePhantom ? '🚫 Phantom Load Hidden' : '👻 Show All (incl. Phantom)'}
                    </span>
                </div>
            </div>

            {/* Power Type Filters */}
            <div className="filter-section">
                <div className="filter-section__title">Power Type</div>
                <div className="filter-bar__row">
                    {technologies.map((tech) => {
                        const short = tech.replace('Battery ', '').replace('Natural ', '').replace('Hybrid (Solar+Storage)', 'Hybrid');
                        return (
                            <button
                                key={tech}
                                className={`filter-chip ${activeTechs.has(tech) ? 'filter-chip--active' : ''}`}
                                onClick={() => toggleTech(tech)}
                            >
                                {short}
                            </button>
                        );
                    })}
                </div>
            </div>

            {/* Provider (ISO) Filters */}
            <div className="filter-section">
                <div className="filter-section__title">Provider (ISO)</div>
                <div className="filter-bar__row">
                    {isos.map((iso) => (
                        <button
                            key={iso}
                            className={`filter-chip ${activeISOs.has(iso) ? 'filter-chip--active' : ''}`}
                            onClick={() => toggleISO(iso)}
                        >
                            {iso}
                        </button>
                    ))}
                </div>
            </div>

            {/* Queue Age Filters */}
            <div className="filter-section">
                <div className="filter-section__title">Years in Queue (Active)</div>
                <div className="filter-bar__row" style={{ display: 'flex', flexWrap: 'nowrap', gap: '4px' }}>
                    {ages.map((age) => (
                        <button
                            key={age}
                            className={`filter-chip ${activeAges.has(age) ? 'filter-chip--active' : ''}`}
                            onClick={() => toggleAge(age)}
                            style={{ flex: 1, padding: '6px 4px', fontSize: '10px', textAlign: 'center', whiteSpace: 'nowrap' }}
                        >
                            {age}
                        </button>
                    ))}
                </div>
            </div>

            {/* Status Filters */}
            <div className="filter-section">
                <div className="filter-section__title">Project Status</div>
                <div className="filter-bar__row">
                    {statuses.map((status) => (
                        <button
                            key={status}
                            className={`filter-chip ${activeStatuses.has(status) ? 'filter-chip--active' : ''} ${status === 'Withdrawn' ? 'filter-chip--danger' : ''
                                }`}
                            onClick={() => toggleStatus(status)}
                        >
                            {status}
                        </button>
                    ))}
                </div>
            </div>
        </div>
    );
}
