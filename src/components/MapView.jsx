import { useRef, useEffect, useMemo } from 'react';
import L from 'leaflet';
import 'leaflet.markercluster';
import usStatesGeoData from '../us-states.json';

/* ── Color helpers ── */
const STATE_COLOR_SCALE = [
    { color: '#1e293b', label: '0 GW' },
    { color: '#93c5fd', label: '< 10 GW' },
    { color: '#3b82f6', label: '10–50 GW' },
    { color: '#6366f1', label: '50–150 GW' },
    { color: '#f97316', label: '150–400 GW' },
    { color: '#ef4444', label: '> 400 GW' },
];

const COUNTY_COLOR_SCALE = [
    { color: '#1e293b', label: '0 MW' },
    { color: '#93c5fd', label: '< 100 MW' },
    { color: '#3b82f6', label: '100–500 MW' },
    { color: '#6366f1', label: '500–2k MW' },
    { color: '#f97316', label: '2–5 GW' },
    { color: '#ef4444', label: '> 5 GW' },
];

function getColorForGW(gw) {
    if (!gw || gw <= 0) return 'transparent';
    if (gw > 400) return '#ef4444';
    if (gw > 150) return '#f97316';
    if (gw > 50) return '#6366f1';
    if (gw > 10) return '#3b82f6';
    return '#93c5fd';
}

function getCountyColor(mw) {
    if (!mw || mw <= 0) return 'transparent';
    if (mw > 5000) return '#ef4444';
    if (mw > 2000) return '#f97316';
    if (mw > 500) return '#6366f1';
    if (mw > 100) return '#3b82f6';
    return '#93c5fd';
}

function getCentroid(feature) {
    const coords = [];
    function extract(c, type) {
        if (type === 'Polygon') c[0].forEach(p => coords.push(p));
        else if (type === 'MultiPolygon') c.forEach(poly => poly[0].forEach(p => coords.push(p)));
    }
    extract(feature.geometry.coordinates, feature.geometry.type);
    if (!coords.length) return [0, 0];
    return [
        coords.reduce((s, c) => s + c[1], 0) / coords.length,
        coords.reduce((s, c) => s + c[0], 0) / coords.length,
    ];
}

export default function MapView({
    geojson,
    stateSummaries,
    countiesGeoJSON,
    onViewportChange,
    onProjectClick,
    onStateSelect,
    onCountySelect,
    selectedState,
    selectedCounty,
}) {
    const mapContainer = useRef(null);
    const mapRef = useRef(null);
    const tooltipRef = useRef(null);
    const legendRef = useRef(null);
    const choroplethRef = useRef(null);
    const countyLayerRef = useRef(null);

    // Stable callback refs
    const cbRefs = useRef({});
    cbRefs.current = { onStateSelect, onCountySelect, onProjectClick, onViewportChange };

    // Props as refs for Leaflet handler access
    const selectedStateRef = useRef(selectedState);
    selectedStateRef.current = selectedState;
    const stateSummariesRef = useRef(stateSummaries);
    stateSummariesRef.current = stateSummaries;
    const geojsonRef = useRef(geojson);
    geojsonRef.current = geojson;

    // Group projects by state for the macro view (reacts to filters)
    const stateData = useMemo(() => {
        if (!geojson?.features) return {};
        const groups = {};
        geojson.features.forEach(f => {
            const p = f.properties;
            const state = p.state;
            if (!state) return;
            if (!groups[state]) {
                groups[state] = { totalMW: 0, count: 0, opMW: 0, activeMW: 0, failedMW: 0 };
            }
            const mw = p.capacity_mw || 0;
            groups[state].count += 1;
            groups[state].totalMW += mw;
            if (p.status === 'Operational') groups[state].opMW += mw;
            else if (p.status === 'Active') groups[state].activeMW += mw;
            else if (['Withdrawn', 'Suspended'].includes(p.status)) groups[state].failedMW += mw;
        });

        Object.values(groups).forEach(g => {
            g.successRate = (g.opMW + g.failedMW) > 0 ? ((g.opMW / (g.opMW + g.failedMW)) * 100).toFixed(1) : '—';
        });

        return groups;
    }, [geojson]);

    const stateDataRef = useRef(stateData);
    stateDataRef.current = stateData;

    // Group projects by county for selected state
    const countyData = useMemo(() => {
        if (!selectedState || !geojson?.features) return {};
        const groups = {};
        geojson.features.forEach(f => {
            const p = f.properties;
            if (p.state !== selectedState) return;
            const county = p.county ? p.county.toUpperCase().replace(/ CO\.?$| COUNTY$| PARISH$/g, '').trim() : 'UNKNOWN';
            if (!groups[county]) {
                groups[county] = { county, totalMW: 0, count: 0, opMW: 0, activeMW: 0, failedMW: 0, projects: [] };
            }
            const mw = p.capacity_mw || 0;
            groups[county].totalMW += mw;
            groups[county].count += 1;
            if (p.status === 'Operational') groups[county].opMW += mw;
            else if (p.status === 'Active') groups[county].activeMW += mw;
            else if (['Withdrawn', 'Suspended'].includes(p.status)) groups[county].failedMW += mw;

            groups[county].projects.push(p);
        });

        Object.values(groups).forEach(g => {
            g.successRate = (g.opMW + g.failedMW) > 0 ? ((g.opMW / (g.opMW + g.failedMW)) * 100).toFixed(1) : '—';
        });

        return groups;
    }, [selectedState, geojson]);

    /* ── Init map + choropleth (ONCE) ── */
    useEffect(() => {
        if (mapRef.current) return;

        const map = L.map(mapContainer.current, {
            center: [39.8, -98.5],
            zoom: 4,
            minZoom: 3,
            maxZoom: 12,
            zoomControl: false,
            attributionControl: false,
            zoomSnap: 0.5,
            zoomDelta: 0.5,
            wheelPxPerZoomLevel: 90,
        });

        L.control.zoom({ position: 'topleft' }).addTo(map);

        // Tooltip element
        const tip = document.createElement('div');
        tip.className = 'map-tooltip';
        tip.style.display = 'none';
        mapContainer.current.appendChild(tip);
        tooltipRef.current = tip;

        // Legend Container
        const legend = document.createElement('div');
        legend.className = 'map-legend';
        mapContainer.current.appendChild(legend);
        legendRef.current = legend;

        // Viewport tracking
        const updateVP = () => cbRefs.current.onViewportChange?.({ zoom: map.getZoom(), bounds: map.getBounds() });
        map.on('moveend', updateVP);
        map.on('zoomend', updateVP);
        setTimeout(updateVP, 300);

        // ── Choropleth with permanent tooltip labels ──
        const stateLayer = L.geoJSON(usStatesGeoData, {
            interactive: true,
            style: (feature) => {
                const abbr = feature.properties.abbr;
                const s = stateDataRef.current?.[abbr];
                const gw = s ? s.totalMW / 1000 : 0;
                const isSel = selectedStateRef.current === abbr;
                return {
                    fillColor: getColorForGW(gw),
                    weight: isSel ? 3 : 1,
                    opacity: isSel ? 0 : (gw > 0 ? 1 : 0.4), // Fade out borders of empty states
                    color: isSel ? 'transparent' : '#334155',
                    fillOpacity: isSel ? 0 : (gw > 0 ? 0.82 : 0), // Fully transparent if 0 GW
                };
            },
            onEachFeature: (feature, layer) => {
                const abbr = feature.properties.abbr;

                // Permanent label using Leaflet tooltip (SVG-based, no offset issues)
                const centroid = getCentroid(feature);
                layer.bindTooltip(abbr, {
                    permanent: true,
                    direction: 'center',
                    className: 'state-abbr-tooltip',
                    interactive: false,
                });

                layer.on({
                    mouseover: (e) => {
                        e.target.setStyle({ weight: 2.5, color: '#f1f5f9', fillOpacity: 1 });
                        e.target.bringToFront();
                        const tip = tooltipRef.current;
                        if (!tip) return;
                        const s = stateDataRef.current?.[abbr];
                        if (s) {
                            // Compute investor metrics from the filtered geojson for this state
                            const opMW = s.opMW;
                            const activeMW = s.activeMW;
                            const successRate = s.successRate;
                            const fmtGW = (mw) => mw >= 1000 ? `${(mw / 1000).toFixed(1)} GW` : `${Math.round(mw)} MW`;
                            const backingSummary = stateSummariesRef.current?.[abbr] || {};
                            tip.innerHTML = `
                                <div class="map-tooltip__title">${feature.properties.name} (${abbr})</div>
                                <div class="map-tooltip__divider"></div>
                                <div class="map-tooltip__row"><span class="map-tooltip__label">⚡ Operational</span><span class="map-tooltip__value">${fmtGW(opMW)}</span></div>
                                <div class="map-tooltip__row"><span class="map-tooltip__label">📊 Success Rate</span><span class="map-tooltip__value">${successRate}${successRate !== '—' ? '%' : ''}</span></div>
                                <div class="map-tooltip__row"><span class="map-tooltip__label">🔄 Active Pipeline</span><span class="map-tooltip__value">${fmtGW(activeMW)}</span></div>
                                <div class="map-tooltip__row"><span class="map-tooltip__label">📉 Filtered Total</span><span class="map-tooltip__value">${fmtGW(s.totalMW)}</span></div>
                                <div class="map-tooltip__divider"></div>
                                <div class="map-tooltip__row" style="opacity:0.7"><span class="map-tooltip__label">Filtered Projects</span><span class="map-tooltip__value">${s.count}</span></div>
                                <div class="map-tooltip__row" style="opacity:0.7"><span class="map-tooltip__label">Top Tech (All-Time)</span><span class="map-tooltip__value">${backingSummary.top_technology || '—'}</span></div>
                            `;
                        } else {
                            tip.innerHTML = `<div class="map-tooltip__title">${feature.properties.name} (${abbr})</div><div style="font-size:11px;color:#94a3b8;">No projects</div>`;
                        }
                        tip.style.display = 'block';
                        const pt = map.latLngToContainerPoint(e.latlng);
                        tip.style.left = pt.x + 15 + 'px';
                        tip.style.top = pt.y - 10 + 'px';
                    },
                    mouseout: (e) => {
                        stateLayer.resetStyle(e.target);
                        if (tooltipRef.current) tooltipRef.current.style.display = 'none';
                    },
                    mousemove: (e) => {
                        const tip = tooltipRef.current;
                        if (!tip) return;
                        const pt = map.latLngToContainerPoint(e.latlng);
                        tip.style.left = pt.x + 15 + 'px';
                        tip.style.top = pt.y - 10 + 'px';
                    },
                    click: (e) => {
                        L.DomEvent.stopPropagation(e);
                        map.fitBounds(e.target.getBounds(), { padding: [30, 30], maxZoom: 7, animate: true });
                        cbRefs.current.onStateSelect?.(abbr);
                        cbRefs.current.onCountySelect?.(null);
                    },
                });
            },
        });

        stateLayer.addTo(map);
        choroplethRef.current = stateLayer;
        mapRef.current = map;

        // Size fix
        requestAnimationFrame(() => map.invalidateSize());
        const onResize = () => map.invalidateSize();
        window.addEventListener('resize', onResize);

        return () => {
            window.removeEventListener('resize', onResize);
            map.remove();
            mapRef.current = null;
        };
    }, []);

    /* ── Update Legend reactively ── */
    useEffect(() => {
        if (!legendRef.current) return;
        const scale = selectedState ? COUNTY_COLOR_SCALE : STATE_COLOR_SCALE;
        const title = selectedState ? 'County Pipeline' : 'State Pipeline (GW)';
        legendRef.current.innerHTML = `
            <div class="map-legend__title">${title}</div>
            <div class="map-legend__scale">
                ${scale.map(c => `<div class="map-legend__item"><span class="map-legend__swatch" style="background:${c.color}${c.color === '#1e293b' ? ';border:1px solid #334155' : ''}"></span><span>${c.label}</span></div>`).join('')}
            </div>
        `;
    }, [selectedState]);

    /* ── Update choropleth styles reactively ── */
    useEffect(() => {
        if (!choroplethRef.current) return;
        choroplethRef.current.setStyle((feature) => {
            const abbr = feature.properties.abbr;
            const s = stateData[abbr];
            const gw = s ? s.totalMW / 1000 : 0;
            const isSel = selectedState === abbr;
            return {
                fillColor: getColorForGW(gw),
                weight: isSel ? 3 : 1,
                opacity: isSel ? 0 : (gw > 0 ? 1 : 0.4),
                color: isSel ? 'transparent' : '#334155',
                fillOpacity: isSel ? 0 : (gw > 0 ? 0.82 : 0),
            };
        });
    }, [selectedState, stateData]);

    /* ── County polygons ── */
    useEffect(() => {
        const map = mapRef.current;
        if (!map) return;

        if (countyLayerRef.current) { map.removeLayer(countyLayerRef.current); countyLayerRef.current = null; }
        if (!selectedState || !countiesGeoJSON) return;

        // Filter counties array for this state
        const stateCounties = {
            type: 'FeatureCollection',
            features: countiesGeoJSON.features.filter(f => f.properties.state_abbr === selectedState)
        };

        const countyLayer = L.geoJSON(stateCounties, {
            style: (feature) => {
                const countyName = feature.properties.county ? feature.properties.county.toUpperCase().replace(/ CO\.?$| COUNTY$| PARISH$/g, '').trim() : '';
                const match = countyData[countyName];
                const mw = match ? match.totalMW : 0;
                const isSelected = selectedCounty === countyName;
                return {
                    fillColor: getCountyColor(mw),
                    weight: isSelected ? 3 : (mw > 0 ? 1 : 0), // No border for empty counties
                    color: isSelected ? '#f8fafc' : 'rgba(255,255,255,0.4)',
                    fillOpacity: mw > 0 ? 0.85 : 0, // Fully transparent if 0 MW
                };
            },
            onEachFeature: (feature, layer) => {
                const countyName = feature.properties.county ? feature.properties.county.toUpperCase().replace(/ CO\.?$| COUNTY$| PARISH$/g, '').trim() : '';
                const displayCountyName = feature.properties.county || 'Unknown'; // For the tooltip
                const match = countyData[countyName];
                const mw = match ? match.totalMW : 0;
                const count = match ? match.count : 0;

                // Permanent label for large counties
                if (mw > 1000) {
                    layer.bindTooltip(`${displayCountyName}<br><span style="opacity:0.8">${(mw / 1000).toFixed(1)}GW</span>`, {
                        permanent: true,
                        direction: 'center',
                        className: 'county-label-tooltip',
                        interactive: false,
                    });
                }

                layer.on({
                    mouseover: (e) => {
                        const tip = tooltipRef.current;
                        if (!tip) return;

                        e.target.setStyle({ weight: 2.5, color: '#f8fafc', fillOpacity: 1 });
                        e.target.bringToFront();

                        const fmtGW = (mw) => mw >= 1000 ? `${(mw / 1000).toFixed(1)} GW` : `${Math.round(mw)} MW`;
                        const opMW = match ? match.opMW : 0;
                        const activeMW = match ? match.activeMW : 0;
                        const successRate = match ? match.successRate : '—';

                        tip.innerHTML = `
                            <div class="map-tooltip__title">${displayCountyName} County, ${selectedState}</div>
                            <div class="map-tooltip__divider"></div>
                            <div class="map-tooltip__row"><span class="map-tooltip__label">⚡ Operational</span><span class="map-tooltip__value">${fmtGW(opMW)}</span></div>
                            <div class="map-tooltip__row"><span class="map-tooltip__label">📊 Success Rate</span><span class="map-tooltip__value">${successRate}${successRate !== '—' ? '%' : ''}</span></div>
                            <div class="map-tooltip__row"><span class="map-tooltip__label">🔄 Active Pipeline</span><span class="map-tooltip__value">${fmtGW(activeMW)}</span></div>
                            <div class="map-tooltip__divider"></div>
                            <div class="map-tooltip__row" style="opacity:0.7"><span class="map-tooltip__label">Projects</span><span class="map-tooltip__value">${count}</span></div>
                        `;
                        tip.style.display = 'block';
                        const pt = map.latLngToContainerPoint(e.latlng);
                        tip.style.left = pt.x + 15 + 'px';
                        tip.style.top = pt.y - 10 + 'px';
                    },
                    mouseout: (e) => {
                        countyLayer.resetStyle(e.target);
                        if (tooltipRef.current) tooltipRef.current.style.display = 'none';
                    },
                    mousemove: (e) => {
                        const tip = tooltipRef.current;
                        if (!tip) return;
                        const pt = map.latLngToContainerPoint(e.latlng);
                        tip.style.left = pt.x + 15 + 'px';
                        tip.style.top = pt.y - 10 + 'px';
                    },
                    click: (e) => {
                        L.DomEvent.stopPropagation(e);
                        cbRefs.current.onCountySelect?.(countyName);
                    }
                });
            }
        });

        countyLayer.addTo(map);
        countyLayerRef.current = countyLayer;

        return () => { if (countyLayerRef.current) { map.removeLayer(countyLayerRef.current); countyLayerRef.current = null; } };
    }, [selectedState, selectedCounty, countyData, countiesGeoJSON]);

    return (
        <div style={{ width: '100%', height: '100%', position: 'relative' }}>
            <div ref={mapContainer} style={{ width: '100%', height: '100%', background: '#0f172a' }} />
            <div style={{ position: 'absolute', top: 15, right: 15, zIndex: 1000 }}>
                <button
                    onClick={() => {
                        const map = mapRef.current;
                        if (map) map.setView([39.8, -98.5], 4, { animate: true });
                        cbRefs.current.onStateSelect?.(null);
                        cbRefs.current.onCountySelect?.(null);
                    }}
                    style={{
                        background: 'var(--accent-blue)',
                        color: 'white',
                        border: '1px solid rgba(255,255,255,0.2)',
                        padding: '8px 12px',
                        borderRadius: '6px',
                        fontWeight: 600,
                        cursor: 'pointer',
                        boxShadow: '0 4px 6px -1px rgba(0,0,0,0.3)',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '6px',
                        transition: 'background 0.2s',
                    }}
                    onMouseOver={(e) => e.currentTarget.style.background = '#2563eb'}
                    onMouseOut={(e) => e.currentTarget.style.background = 'var(--accent-blue)'}
                >
                    <span style={{ fontSize: 16 }}>🇺🇸</span> Zoom to US
                </button>
            </div>
        </div>
    );
}
