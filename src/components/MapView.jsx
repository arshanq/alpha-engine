import { useRef, useEffect, useMemo } from 'react';
import L from 'leaflet';
import 'leaflet.markercluster';
import usStatesGeoData from '../us-states.json';

/* ── Color helpers ── */
const COLOR_SCALE = [
    { color: '#1e293b', label: '0 GW' },
    { color: '#93c5fd', label: '< 10 GW' },
    { color: '#3b82f6', label: '10–50 GW' },
    { color: '#6366f1', label: '50–150 GW' },
    { color: '#f97316', label: '150–400 GW' },
    { color: '#ef4444', label: '> 400 GW' },
];

function getColorForGW(gw) {
    if (!gw || gw <= 0) return '#1e293b';
    if (gw > 400) return '#ef4444';
    if (gw > 150) return '#f97316';
    if (gw > 50) return '#6366f1';
    if (gw > 10) return '#3b82f6';
    return '#93c5fd';
}

function getCountyColor(mw) {
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

    // Group projects by county for selected state
    const countyData = useMemo(() => {
        if (!selectedState || !geojson?.features) return {};
        const groups = {};
        geojson.features.forEach(f => {
            const p = f.properties;
            if (p.state !== selectedState) return;
            const county = p.county || 'Unknown';
            if (!groups[county]) {
                groups[county] = { county, totalMW: 0, count: 0, lats: [], lngs: [], projects: [] };
            }
            groups[county].totalMW += p.capacity_mw || 0;
            groups[county].count += 1;
            const [lng, lat] = f.geometry.coordinates;
            if (lat && lng) { groups[county].lats.push(lat); groups[county].lngs.push(lng); }
            groups[county].projects.push(p);
        });
        Object.values(groups).forEach(g => {
            if (g.lats.length) {
                g.lat = g.lats.reduce((a, b) => a + b, 0) / g.lats.length;
                g.lng = g.lngs.reduce((a, b) => a + b, 0) / g.lngs.length;
            }
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

        // Reset button
        const resetCtl = L.control({ position: 'topleft' });
        resetCtl.onAdd = () => {
            const div = L.DomUtil.create('div', 'leaflet-bar leaflet-control');
            div.innerHTML = '<a href="#" title="Reset to US" style="font-size:16px;line-height:30px;width:30px;text-align:center;display:block;text-decoration:none;color:#e2e8f0;">🇺🇸</a>';
            div.onclick = (e) => {
                e.preventDefault(); e.stopPropagation();
                map.setView([39.8, -98.5], 4, { animate: true });
                cbRefs.current.onStateSelect?.(null);
                cbRefs.current.onCountySelect?.(null);
            };
            return div;
        };
        resetCtl.addTo(map);

        // Tooltip element
        const tip = document.createElement('div');
        tip.className = 'map-tooltip';
        tip.style.display = 'none';
        mapContainer.current.appendChild(tip);
        tooltipRef.current = tip;

        // Legend
        const legend = document.createElement('div');
        legend.className = 'map-legend';
        legend.innerHTML = `
            <div class="map-legend__title">Total Pipeline (GW)</div>
            <div class="map-legend__scale">
                ${COLOR_SCALE.map(c => `<div class="map-legend__item"><span class="map-legend__swatch" style="background:${c.color}${c.color === '#1e293b' ? ';border:1px solid #334155' : ''}"></span><span>${c.label}</span></div>`).join('')}
            </div>
        `;
        mapContainer.current.appendChild(legend);

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
                const s = stateSummariesRef.current?.[abbr];
                const gw = s ? s.total_gw : 0;
                const isSel = selectedStateRef.current === abbr;
                return {
                    fillColor: getColorForGW(gw),
                    weight: isSel ? 3 : 1,
                    opacity: 1,
                    color: isSel ? '#38bdf8' : '#334155',
                    fillOpacity: gw > 0 ? 0.82 : 0.3,
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
                        const summary = stateSummariesRef.current?.[abbr];
                        if (summary) {
                            // Compute investor metrics from geojson for this state
                            const geoRef = geojsonRef.current;
                            const stateProjects = geoRef?.features?.filter(f => f.properties.state === abbr) || [];
                            const opProjects = stateProjects.filter(f => f.properties.status === 'Operational');
                            const activeProjects = stateProjects.filter(f => f.properties.status === 'Active');
                            const failedProjects = stateProjects.filter(f => f.properties.status === 'Withdrawn' || f.properties.status === 'Suspended');
                            const opMW = opProjects.reduce((s, f) => s + (f.properties.capacity_mw || 0), 0);
                            const activeMW = activeProjects.reduce((s, f) => s + (f.properties.capacity_mw || 0), 0);
                            const failedMW = failedProjects.reduce((s, f) => s + (f.properties.capacity_mw || 0), 0);
                            const successRate = (opMW + failedMW) > 0 ? ((opMW / (opMW + failedMW)) * 100).toFixed(1) : '—';
                            const fmtGW = (mw) => mw >= 1000 ? `${(mw / 1000).toFixed(1)} GW` : `${Math.round(mw)} MW`;
                            tip.innerHTML = `
                                <div class="map-tooltip__title">${feature.properties.name} (${abbr})</div>
                                <div class="map-tooltip__divider"></div>
                                <div class="map-tooltip__row"><span class="map-tooltip__label">⚡ Operational</span><span class="map-tooltip__value">${fmtGW(opMW)}</span></div>
                                <div class="map-tooltip__row"><span class="map-tooltip__label">📊 Success Rate</span><span class="map-tooltip__value">${successRate}%</span></div>
                                <div class="map-tooltip__row"><span class="map-tooltip__label">🔄 Active Pipeline</span><span class="map-tooltip__value">${fmtGW(activeMW)}</span></div>
                                <div class="map-tooltip__divider"></div>
                                <div class="map-tooltip__row" style="opacity:0.7"><span class="map-tooltip__label">Projects</span><span class="map-tooltip__value">${summary.project_count}</span></div>
                                <div class="map-tooltip__row" style="opacity:0.7"><span class="map-tooltip__label">Top Tech</span><span class="map-tooltip__value">${summary.top_technology}</span></div>
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

        return () => { window.removeEventListener('resize', onResize); map.remove(); mapRef.current = null; };
    }, []);

    /* ── Update choropleth styles reactively ── */
    useEffect(() => {
        if (!choroplethRef.current) return;
        choroplethRef.current.setStyle((feature) => {
            const abbr = feature.properties.abbr;
            const s = stateSummaries?.[abbr];
            const gw = s ? s.total_gw : 0;
            const isSel = selectedState === abbr;
            return {
                fillColor: getColorForGW(gw),
                weight: isSel ? 3 : 1,
                opacity: 1,
                color: isSel ? '#38bdf8' : '#334155',
                fillOpacity: gw > 0 ? 0.82 : 0.3,
            };
        });
    }, [selectedState, stateSummaries]);

    /* ── County bubbles using circleMarker (SVG-based, no offset!) ── */
    useEffect(() => {
        const map = mapRef.current;
        if (!map) return;

        if (countyLayerRef.current) { map.removeLayer(countyLayerRef.current); countyLayerRef.current = null; }
        if (!selectedState || !Object.keys(countyData).length) return;

        const countyGroup = L.layerGroup();
        const maxCountyMW = Math.max(100, ...Object.values(countyData).map(c => c.totalMW));

        Object.values(countyData).forEach(county => {
            if (!county.lat || !county.lng) return;

            const mw = county.totalMW;
            const isSelected = selectedCounty === county.county;
            const radiusBase = Math.max(8, Math.min(25, 8 + (mw / maxCountyMW) * 17));
            const radius = isSelected ? radiusBase + 4 : radiusBase;
            const color = getCountyColor(mw);

            // Use circleMarker — SVG-based, renders in the same coordinate system as GeoJSON
            const circle = L.circleMarker([county.lat, county.lng], {
                radius: radius,
                fillColor: color,
                fillOpacity: 0.85,
                color: isSelected ? '#f1f5f9' : 'rgba(255,255,255,0.4)',
                weight: isSelected ? 3 : 1.5,
                interactive: true,
            });

            // County name label
            const label = mw >= 1000 ? `${county.county} (${(mw / 1000).toFixed(1)}G)` : `${county.county} (${Math.round(mw)}MW)`;
            circle.bindTooltip(label, {
                permanent: true,
                direction: 'top',
                offset: [0, -radius - 2],
                className: 'county-label-tooltip',
                interactive: false,
            });

            // Hover detail tooltip
            circle.on('mouseover', (e) => {
                const tip = tooltipRef.current;
                if (!tip) return;
                const avgSuccess = county.projects.length
                    ? (county.projects.reduce((s, p) => s + (p.success_probability || 0), 0) / county.projects.length * 100).toFixed(0)
                    : '—';
                tip.innerHTML = `
                    <div class="map-tooltip__title">${county.county} County, ${selectedState}</div>
                    <div class="map-tooltip__row"><span class="map-tooltip__label">Capacity</span><span class="map-tooltip__value">${mw >= 1000 ? (mw / 1000).toFixed(1) + ' GW' : Math.round(mw) + ' MW'}</span></div>
                    <div class="map-tooltip__row"><span class="map-tooltip__label">Projects</span><span class="map-tooltip__value">${county.count}</span></div>
                    <div class="map-tooltip__row"><span class="map-tooltip__label">Avg Success</span><span class="map-tooltip__value">${avgSuccess}%</span></div>
                `;
                tip.style.display = 'block';
                const pt = map.latLngToContainerPoint(e.latlng);
                tip.style.left = pt.x + 15 + 'px';
                tip.style.top = pt.y - 10 + 'px';

                // Pulse effect
                circle.setStyle({ fillOpacity: 1, weight: 3, color: '#38bdf8' });
            });

            circle.on('mouseout', () => {
                if (tooltipRef.current) tooltipRef.current.style.display = 'none';
                circle.setStyle({
                    fillOpacity: 0.85,
                    weight: isSelected ? 3 : 1.5,
                    color: isSelected ? '#f1f5f9' : 'rgba(255,255,255,0.4)',
                });
            });

            circle.on('click', (e) => {
                L.DomEvent.stopPropagation(e);
                cbRefs.current.onCountySelect?.(county.county);
            });

            countyGroup.addLayer(circle);
        });

        countyGroup.addTo(map);
        countyLayerRef.current = countyGroup;

        return () => { if (countyLayerRef.current) { map.removeLayer(countyLayerRef.current); countyLayerRef.current = null; } };
    }, [selectedState, selectedCounty, countyData]);

    return <div ref={mapContainer} style={{ width: '100%', height: '100%', background: '#0f172a', position: 'relative' }} />;
}
