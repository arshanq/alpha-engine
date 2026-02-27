import { useState, useEffect, useCallback, useMemo } from 'react';
import MapView from './components/MapView';
import Sidebar from './components/Sidebar';
import FilterBar from './components/FilterBar';
import ProjectDetail from './components/ProjectDetail';

const ISOS = ['CAISO', 'MISO', 'SPP', 'ERCOT', 'NYISO', 'ISONE'];
const TECHNOLOGIES = ['Solar', 'Wind', 'Battery Storage', 'Natural Gas', 'Hybrid', 'Nuclear', 'Hydro', 'Coal', 'Other'];
const STATUSES = ['Active', 'Operational', 'Withdrawn', 'Suspended'];
const AGES = ['< 1 Year', '1-2 Years', '2-5 Years', '> 5 Years'];

export default function App() {
    const [allProjects, setAllProjects] = useState(null);
    const [stateSummaries, setStateSummaries] = useState(null);
    const [countiesGeoJSON, setCountiesGeoJSON] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    // Filters
    const [activeISOs, setActiveISOs] = useState(new Set(ISOS));
    const [activeTechs, setActiveTechs] = useState(new Set(TECHNOLOGIES));
    const [activeStatuses, setActiveStatuses] = useState(new Set(STATUSES));
    const [activeAges, setActiveAges] = useState(new Set(AGES));
    const [hidePhantom, setHidePhantom] = useState(false);

    // Map / selection state
    const [viewport, setViewport] = useState({ zoom: 4, bounds: null });
    const [selectedProject, setSelectedProject] = useState(null);
    const [selectedState, setSelectedState] = useState(null);
    const [selectedCounty, setSelectedCounty] = useState(null);

    // Layout state
    const [summaryWidth, setSummaryWidth] = useState(380);
    const [isResizing, setIsResizing] = useState(false);
    const [filtersCollapsed, setFiltersCollapsed] = useState(false);

    // Fetch data
    useEffect(() => {
        async function fetchData() {
            try {
                const [queueRes, summaryRes, countiesRes] = await Promise.all([
                    fetch('/api/queue/geojson'),
                    fetch('/api/queue/summary'),
                    fetch('/counties.geojson'),
                ]);
                if (!queueRes.ok || !summaryRes.ok || !countiesRes.ok) throw new Error('API fetch failed');
                const geojson = await queueRes.json();
                const summaries = await summaryRes.json();
                const counties = await countiesRes.json();
                setAllProjects(geojson);
                setStateSummaries(summaries);
                setCountiesGeoJSON(counties);
            } catch (err) {
                setError(err.message);
            } finally {
                setLoading(false);
            }
        }
        fetchData();
    }, []);

    // Filter projects (global filter, unrelated to map selection)
    const filtered = useMemo(() => {
        if (!allProjects) return null;
        const features = allProjects.features.filter((f) => {
            const p = f.properties;
            if (!activeISOs.has(p.iso)) return false;
            if (!activeTechs.has(p.technology)) return false;
            if (!activeStatuses.has(p.status)) return false;
            if (hidePhantom && p.is_phantom) return false;

            // Age filter logic
            const qDays = p.queue_days || 0;
            if (activeAges.size < AGES.length) {
                let ageMatch = false;
                if (activeAges.has('< 1 Year') && qDays <= 365) ageMatch = true;
                if (activeAges.has('1-2 Years') && qDays > 365 && qDays <= 730) ageMatch = true;
                if (activeAges.has('2-5 Years') && qDays > 730 && qDays <= 1825) ageMatch = true;
                if (activeAges.has('> 5 Years') && qDays > 1825) ageMatch = true;

                // If it's withdrawn/operational, we skip age filtering (as they already resolved)
                if (p.status !== 'Active' && p.status !== 'Suspended') {
                    ageMatch = true;
                }

                if (!ageMatch) return false;
            }

            return true;
        });
        return { ...allProjects, features };
    }, [allProjects, activeISOs, activeTechs, activeStatuses, hidePhantom, activeAges]);

    // ── Scoped projects: context-aware based on map selection ──
    // Default = all filtered | State selected = that state only | County selected = that county only
    const scopedProjects = useMemo(() => {
        if (!filtered) return [];
        let projects = filtered.features.map(f => f.properties);

        if (selectedState) {
            projects = projects.filter(p => p.state === selectedState);
        }
        if (selectedCounty) {
            projects = projects.filter(p => p.county === selectedCounty);
        }
        return projects;
    }, [filtered, selectedState, selectedCounty]);

    // Compute investor-focused stats from scoped projects
    const stats = useMemo(() => {
        if (!scopedProjects.length) {
            return {
                totalMW: 0, count: 0, avgSuccess: 0, medianWait: 0, phantom: 0,
                operationalMW: 0, operationalCount: 0,
                activeMW: 0, activeCount: 0,
                withdrawnMW: 0, withdrawnCount: 0,
                historicalSuccessRate: 0,
                historicalSuccessRateByCount: 0,
            };
        }

        const operational = scopedProjects.filter(p => p.status === 'Operational');
        const active = scopedProjects.filter(p => p.status === 'Active');
        const withdrawn = scopedProjects.filter(p => p.status === 'Withdrawn' || p.status === 'Suspended');

        const operationalMW = operational.reduce((s, p) => s + (p.capacity_mw || 0), 0);
        const activeMW = active.reduce((s, p) => s + (p.capacity_mw || 0), 0);
        const withdrawnMW = withdrawn.reduce((s, p) => s + (p.capacity_mw || 0), 0);

        const totalMW = scopedProjects.reduce((s, p) => s + (p.capacity_mw || 0), 0);
        const successScores = scopedProjects.map(p => p.success_probability || 0);
        const queueDays = scopedProjects.map(p => p.queue_days || 0).sort((a, b) => a - b);
        const phantom = scopedProjects.filter(p => p.is_phantom).length;

        // Historical success: what fraction of resolved projects actually went operational
        const resolvedCount = operational.length + withdrawn.length;
        const resolvedMW = operationalMW + withdrawnMW;
        const historicalSuccessRate = resolvedMW > 0 ? operationalMW / resolvedMW : 0;
        const historicalSuccessRateByCount = resolvedCount > 0 ? operational.length / resolvedCount : 0;

        return {
            totalMW,
            count: scopedProjects.length,
            avgSuccess: successScores.reduce((a, b) => a + b, 0) / successScores.length,
            medianWait: queueDays[Math.floor(queueDays.length / 2)] || 0,
            phantom,
            operationalMW,
            operationalCount: operational.length,
            activeMW,
            activeCount: active.length,
            withdrawnMW,
            withdrawnCount: withdrawn.length,
            historicalSuccessRate,
            historicalSuccessRateByCount,
        };
    }, [scopedProjects]);

    // Build a scope label for the sidebar header
    const scopeLabel = useMemo(() => {
        if (selectedCounty && selectedState) return `${selectedCounty}, ${selectedState}`;
        if (selectedState) return selectedState;
        return 'All US';
    }, [selectedState, selectedCounty]);

    // When state changes, clear county selection
    const handleStateSelect = useCallback((abbr) => {
        setSelectedState(abbr);
        setSelectedCounty(null);
    }, []);

    // When county is selected (state is already set by the map)
    const handleCountySelect = useCallback((county) => {
        setSelectedCounty(county);
    }, []);

    // Toggle helpers
    const toggleISO = (iso) => {
        setActiveISOs((prev) => { const n = new Set(prev); n.has(iso) ? n.delete(iso) : n.add(iso); return n; });
    };
    const toggleTech = (tech) => {
        setActiveTechs((prev) => { const n = new Set(prev); n.has(tech) ? n.delete(tech) : n.add(tech); return n; });
    };
    const toggleStatus = (status) => {
        setActiveStatuses((prev) => { const n = new Set(prev); n.has(status) ? n.delete(status) : n.add(status); return n; });
    };
    const toggleAge = (age) => {
        setActiveAges((prev) => { const n = new Set(prev); n.has(age) ? n.delete(age) : n.add(age); return n; });
    };

    if (loading) {
        return (
            <div className="loading-overlay">
                <div className="loading-spinner" />
                <div className="loading-text">Loading Infrastructure Intelligence...</div>
            </div>
        );
    }

    if (error) {
        return (
            <div className="loading-overlay">
                <div style={{ fontSize: 36 }}>⚡</div>
                <div className="loading-text">Failed to load data: {error}</div>
                <div className="loading-text" style={{ fontSize: 11 }}>
                    Make sure the backend is running: <code>uvicorn server.main:app --reload</code>
                </div>
            </div>
        );
    }

    const handleResizeStart = (e) => {
        e.preventDefault();
        setIsResizing(true);
        const startX = e.clientX;
        const startWidth = summaryWidth;

        const handleMouseMove = (e) => {
            const delta = startX - e.clientX;
            setSummaryWidth(Math.max(300, Math.min(600, startWidth + delta)));
        };
        const handleMouseUp = () => {
            setIsResizing(false);
            document.removeEventListener('mousemove', handleMouseMove);
            document.removeEventListener('mouseup', handleMouseUp);
            document.body.style.cursor = '';
            document.body.style.userSelect = '';
        };
        document.addEventListener('mousemove', handleMouseMove);
        document.addEventListener('mouseup', handleMouseUp);
        document.body.style.cursor = 'col-resize';
        document.body.style.userSelect = 'none';
    };

    return (
        <div className="app">
            <header className="header">
                <div className="header__logo">
                    <div className="header__icon">⚡</div>
                    <div>
                        <div className="header__title">Alpha Engine</div>
                        <div className="header__subtitle">Infrastructure Intelligence</div>
                    </div>
                </div>
                <div className="header__stats">
                    <div className="header__stat">
                        <div className="header__stat-value">{(stats.operationalMW / 1000).toFixed(1)} GW</div>
                        <div className="header__stat-label">Operational</div>
                    </div>
                    <div className="header__stat">
                        <div className="header__stat-value">{(stats.historicalSuccessRate * 100).toFixed(0)}%</div>
                        <div className="header__stat-label">Success Rate</div>
                    </div>
                    <div className="header__stat">
                        <div className="header__stat-value">{(stats.activeMW / 1000).toFixed(1)} GW</div>
                        <div className="header__stat-label">Pipeline</div>
                    </div>
                    <div className="header__stat">
                        <div className="header__stat-value">{stats.count.toLocaleString()}</div>
                        <div className="header__stat-label">Total</div>
                    </div>
                </div>
            </header>

            <div className="main">
                {/* ── Left: Filter Panel ── */}
                <div className={`filter-panel ${filtersCollapsed ? 'filter-panel--collapsed' : ''}`}>
                    <button
                        className="filter-panel__toggle"
                        onClick={() => setFiltersCollapsed(!filtersCollapsed)}
                        title={filtersCollapsed ? 'Show Filters' : 'Hide Filters'}
                    >
                        {filtersCollapsed ? '▶' : '◀'}
                    </button>
                    {!filtersCollapsed && (
                        <div className="filter-panel__content">
                            <FilterBar
                                isos={ISOS} activeISOs={activeISOs} toggleISO={toggleISO}
                                technologies={TECHNOLOGIES} activeTechs={activeTechs} toggleTech={toggleTech}
                                statuses={STATUSES} activeStatuses={activeStatuses} toggleStatus={toggleStatus}
                                ages={AGES} activeAges={activeAges} toggleAge={toggleAge}
                                hidePhantom={hidePhantom} setHidePhantom={setHidePhantom}
                            />
                        </div>
                    )}
                </div>

                {/* ── Center: Map ── */}
                <div className="map-container">
                    <MapView
                        geojson={filtered}
                        countiesGeoJSON={countiesGeoJSON}
                        stateSummaries={stateSummaries}
                        onViewportChange={setViewport}
                        onProjectClick={setSelectedProject}
                        onStateSelect={handleStateSelect}
                        onCountySelect={handleCountySelect}
                        selectedState={selectedState}
                        selectedCounty={selectedCounty}
                    />
                    <div className="map-zoom-indicator">
                        Zoom {viewport.zoom?.toFixed(1)} •{' '}
                        {viewport.zoom < 5 ? 'State View' : viewport.zoom < 8 ? 'County View' : 'Project View'}
                    </div>
                </div>

                <ProjectDetail
                    project={selectedProject}
                    onClose={() => setSelectedProject(null)}
                />

                {/* ── Right: Resizable Summary Panel ── */}
                <div
                    className="resize-handle"
                    onMouseDown={handleResizeStart}
                    title="Drag to resize"
                />
                <div className="sidebar" style={{ width: summaryWidth, maxWidth: summaryWidth, minWidth: 300 }}>
                    <Sidebar
                        stats={stats}
                        scopedProjects={scopedProjects}
                        scopeLabel={scopeLabel}
                        stateSummaries={stateSummaries}
                        selectedState={selectedState}
                        selectedCounty={selectedCounty}
                        viewport={viewport}
                    />
                </div>
            </div>
        </div>
    );
}
