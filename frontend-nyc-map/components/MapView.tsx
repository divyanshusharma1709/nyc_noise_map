"use client";

import type { LatLngBounds, LeafletEvent } from "leaflet";
import { useEffect, useMemo, useState } from "react";
import {
  Circle,
  CircleMarker,
  MapContainer,
  Popup,
  TileLayer,
  useMapEvents,
} from "react-leaflet";
import "leaflet/dist/leaflet.css";

type Complaint = {
  unique_key: string;
  created_date: string;
  complaint_type: string;
  descriptor: string | null;
  borough: string | null;
  incident_address: string | null;
  lat: number;
  lon: number;
  status: string | null;
};

type GridCell = {
  lat: number;
  lon: number;
  count: number;
  lat_bucket: number;
  lon_bucket: number;
  zoom_level?: number | null;
  complaint_type?: string | null;
  borough?: string | null;
};

type OverviewStats = {
  overview: {
    total_complaints: number;
    complaint_types: number;
    boroughs: number;
    earliest_created_date: string | null;
    latest_created_date: string | null;
  };
  top_complaint_types: Array<{ complaint_type: string; count: number }>;
  counts_by_borough: Array<{ borough: string | null; count: number }>;
};

type SyncRun = {
  sync_name: string;
  last_started_at: string | null;
  last_finished_at: string | null;
  last_successful_at: string | null;
  last_status: string | null;
  last_error: string | null;
  rows_processed: number;
};

type BoundsState = {
  minLat: number;
  maxLat: number;
  minLon: number;
  maxLon: number;
};

const API_BASE = process.env.NEXT_PUBLIC_API_BASE;

if (!API_BASE) {
  throw new Error("NEXT_PUBLIC_API_BASE is not configured");
}
const DEFAULT_CENTER: [number, number] = [40.7128, -74.006];
const DEFAULT_BOUNDS: BoundsState = {
  minLat: 40.4774,
  maxLat: 40.9176,
  minLon: -74.2591,
  maxLon: -73.7002,
};

const complaintColors: Record<string, string> = {
  "Noise - Residential": "#2563eb",
  "Noise - Commercial": "#dc2626",
  "Noise - Street/Sidewalk": "#16a34a",
  "Noise - Vehicle": "#d97706",
  default: "#475569",
};

const boroughOptions = ["ALL", "BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND"];
const GRID_ONLY_MAX_ZOOM = 14;
const MIXED_MODE_MIN_ZOOM = 15;
const POINTS_ONLY_MIN_ZOOM = 17;

function formatDate(value: string | null) {
  if (!value) return "n/a";
  return new Date(value).toLocaleString();
}

function buildQuery(params: Record<string, string | number | null | undefined>) {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      query.set(key, String(value));
    }
  });
  return query.toString();
}

function MapEvents({
  onViewportChange,
}: {
  onViewportChange: (zoom: number, bounds: BoundsState) => void;
}) {
  const syncViewport = (event: LeafletEvent) => {
    const map = event.target;
    const bounds: LatLngBounds = map.getBounds();
    onViewportChange(map.getZoom(), {
      minLat: bounds.getSouth(),
      maxLat: bounds.getNorth(),
      minLon: bounds.getWest(),
      maxLon: bounds.getEast(),
    });
  };

  useMapEvents({
    moveend: syncViewport,
    zoomend: syncViewport,
  });
  return null;
}

export default function MapView() {
  const [complaints, setComplaints] = useState<Complaint[]>([]);
  const [gridData, setGridData] = useState<GridCell[]>([]);
  const [overview, setOverview] = useState<OverviewStats | null>(null);
  const [syncRuns, setSyncRuns] = useState<SyncRun[]>([]);
  const [zoomLevel, setZoomLevel] = useState(11);
  const [bounds, setBounds] = useState<BoundsState>(DEFAULT_BOUNDS);
  const [borough, setBorough] = useState("ALL");
  const [complaintType, setComplaintType] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const visibleComplaintType = complaintType.trim();
  const showGrid = zoomLevel <= POINTS_ONLY_MIN_ZOOM - 1;
  const shouldLoadPoints = zoomLevel >= MIXED_MODE_MIN_ZOOM;

  useEffect(() => {
    const controller = new AbortController();

    async function loadSummary() {
      try {
        const [overviewResponse, syncResponse] = await Promise.all([
          fetch(`${API_BASE}/stats/overview`, { signal: controller.signal }),
          fetch(`${API_BASE}/sync/status`, { signal: controller.signal }),
        ]);

        if (!overviewResponse.ok || !syncResponse.ok) {
          throw new Error("Failed to load dashboard metadata.");
        }

        const [overviewJson, syncJson] = await Promise.all([
          overviewResponse.json(),
          syncResponse.json(),
        ]);

        setOverview(overviewJson);
        setSyncRuns(Array.isArray(syncJson) ? syncJson : []);
      } catch (fetchError) {
        if (!controller.signal.aborted) {
          setError(fetchError instanceof Error ? fetchError.message : "Failed to load metadata.");
        }
      }
    }

    loadSummary();
    return () => controller.abort();
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    const sharedQuery = {
      borough: borough === "ALL" ? null : borough,
      complaint_type: visibleComplaintType || null,
      min_lat: bounds.minLat,
      max_lat: bounds.maxLat,
      min_lon: bounds.minLon,
      max_lon: bounds.maxLon,
    };
    const complaintsQuery = buildQuery({
      ...sharedQuery,
      limit: 250,
    });
    const gridQuery = buildQuery({
      ...sharedQuery,
      min_count: 2,
      zoom_level: zoomLevel,
    });

    async function loadMapData() {
      setLoading(true);
      setError(null);

      try {
        const gridPromise = fetch(`${API_BASE}/grid-summary?${gridQuery}`, {
          signal: controller.signal,
        });
        const complaintsPromise = shouldLoadPoints
          ? fetch(`${API_BASE}/complaints?${complaintsQuery}`, {
              signal: controller.signal,
            })
          : Promise.resolve(null);

        const [complaintsResponse, gridResponse] = await Promise.all([
          complaintsPromise,
          gridPromise,
        ]);

        if (!gridResponse.ok || (complaintsResponse && !complaintsResponse.ok)) {
          throw new Error("Failed to load map data.");
        }

        const gridJson = await gridResponse.json();
        setGridData(Array.isArray(gridJson) ? gridJson : []);

        if (complaintsResponse) {
          const complaintsJson = await complaintsResponse.json();
          setComplaints(Array.isArray(complaintsJson) ? complaintsJson : []);
        } else {
          setComplaints([]);
        }
      } catch (fetchError) {
        if (!controller.signal.aborted) {
          setError(fetchError instanceof Error ? fetchError.message : "Failed to load map data.");
          setComplaints([]);
          setGridData([]);
        }
      } finally {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      }
    }

    const timeoutId = window.setTimeout(loadMapData, 350);
    return () => {
      controller.abort();
      window.clearTimeout(timeoutId);
    };
  }, [borough, bounds, shouldLoadPoints, visibleComplaintType]);

  const complaintCounts = gridData.map((cell) => Number(cell.count)).filter((count) => !Number.isNaN(count));
  const averageGridCount = complaintCounts.length
    ? complaintCounts.reduce((sum, count) => sum + count, 0) / complaintCounts.length
    : 0;

  const topComplaintTypes = useMemo(() => {
    return overview?.top_complaint_types.slice(0, 5) ?? [];
  }, [overview]);

  const latestSync = syncRuns[0] ?? null;

  const getGridColor = (count: number) => {
    if (count > averageGridCount * 2.5) return "#b91c1c";
    if (count > averageGridCount) return "#f59e0b";
    return "#15803d";
  };

  return (
    <div className="map-shell">
      <aside className="control-panel">
        <div className="panel-block">
          <p className="eyebrow">NYC 311 Noise Map</p>
          <h1>Live complaint activity</h1>
          <p className="muted">
            The map now reads complaint points, density circles, overview stats, and sync health from the backend.
          </p>
        </div>

        <div className="panel-block">
          <label className="field-label" htmlFor="borough-filter">
            Borough
          </label>
          <select
            id="borough-filter"
            className="field-input"
            value={borough}
            onChange={(event) => setBorough(event.target.value)}
          >
            {boroughOptions.map((option) => (
              <option key={option} value={option}>
                {option === "ALL" ? "All boroughs" : option}
              </option>
            ))}
          </select>

          <label className="field-label" htmlFor="complaint-filter">
            Complaint type
          </label>
          <input
            id="complaint-filter"
            className="field-input"
            placeholder="Noise - Residential"
            value={complaintType}
            onChange={(event) => setComplaintType(event.target.value)}
          />
        </div>

        <div className="stats-grid">
          <div className="stat-card">
            <span className="stat-label">Visible points</span>
            <strong>{complaints.length}</strong>
          </div>
          <div className="stat-card">
            <span className="stat-label">Visible cells</span>
            <strong>{gridData.length}</strong>
          </div>
          <div className="stat-card">
            <span className="stat-label">Dataset rows</span>
            <strong>{overview?.overview.total_complaints ?? "..."}</strong>
          </div>
          <div className="stat-card">
            <span className="stat-label">Latest complaint</span>
            <strong>{overview ? formatDate(overview.overview.latest_created_date) : "..."}</strong>
          </div>
        </div>

        <div className="panel-block">
          <h2>Sync status</h2>
          <p className="muted">
            Last success: {latestSync ? formatDate(latestSync.last_successful_at) : "No sync recorded"}
          </p>
          <p className="muted">Rows processed: {latestSync?.rows_processed ?? 0}</p>
          <p className="muted status-line">
            Status: <span data-status={latestSync?.last_status ?? "unknown"}>{latestSync?.last_status ?? "unknown"}</span>
          </p>
        </div>

        <div className="panel-block">
          <h2>Top complaint types</h2>
          <div className="list-block">
            {topComplaintTypes.map((item) => (
              <div className="list-row" key={item.complaint_type}>
                <span>{item.complaint_type}</span>
                <strong>{item.count}</strong>
              </div>
            ))}
            {!topComplaintTypes.length && <p className="muted">No summary data available.</p>}
          </div>
        </div>

        <div className="panel-block legend-block">
          <h2>Density legend</h2>
          <div className="legend-row">
            <span className="legend-dot low" />
            <span>Below viewport average</span>
          </div>
          <div className="legend-row">
            <span className="legend-dot medium" />
            <span>Above viewport average</span>
          </div>
          <div className="legend-row">
            <span className="legend-dot high" />
            <span>2.5x viewport average</span>
          </div>
          <p className="muted">Average complaints per visible cell: {averageGridCount.toFixed(1)}</p>
        </div>

        {loading && <p className="muted">Loading map data...</p>}
        {error && <p className="error-text">{error}</p>}
      </aside>

      <div className="map-stage">
        <MapContainer center={DEFAULT_CENTER} zoom={zoomLevel} preferCanvas={true} style={{ height: "100%", width: "100%" }}>
          <TileLayer attribution="&copy; OpenStreetMap" url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
          <MapEvents
            onViewportChange={(nextZoom, nextBounds) => {
              setZoomLevel((currentZoom) => (currentZoom === nextZoom ? currentZoom : nextZoom));
              setBounds((currentBounds) => {
                if (
                  currentBounds.minLat === nextBounds.minLat &&
                  currentBounds.maxLat === nextBounds.maxLat &&
                  currentBounds.minLon === nextBounds.minLon &&
                  currentBounds.maxLon === nextBounds.maxLon
                ) {
                  return currentBounds;
                }
                return nextBounds;
              });
            }}
          />

          {showGrid && gridData.map((cell, idx) => {
            const color = getGridColor(Number(cell.count));
            return (
              <Circle
                key={`${cell.lat_bucket}-${cell.lon_bucket}-${cell.count}-${cell.zoom_level ?? "live"}-${cell.borough ?? "all"}-${cell.complaint_type ?? "all"}-${idx}`}
                center={[Number(cell.lat), Number(cell.lon)]}
                radius={250}
                pathOptions={{
                  color,
                  fillColor: color,
                  fillOpacity: 0.25,
                  weight: 1,
                }}
              >
                <Popup>
                  <strong>{cell.count} complaints</strong>
                  <br />
                  Grid center: {Number(cell.lat).toFixed(4)}, {Number(cell.lon).toFixed(4)}
                </Popup>
              </Circle>
            );
          })}

          {shouldLoadPoints &&
            complaints
              .filter((item) => Number.isFinite(item.lat) && Number.isFinite(item.lon))
              .map((item) => (
                <CircleMarker
                  key={item.unique_key}
                  center={[item.lat, item.lon]}
                  radius={5}
                  pathOptions={{
                    color: complaintColors[item.complaint_type] ?? complaintColors.default,
                    fillColor: complaintColors[item.complaint_type] ?? complaintColors.default,
                    fillOpacity: 0.9,
                    weight: 1,
                  }}
                >
                  <Popup>
                    <strong>{item.complaint_type}</strong>
                    <br />
                    {item.descriptor ?? "No descriptor"}
                    <br />
                    {formatDate(item.created_date)}
                    <br />
                    {item.incident_address ?? item.borough ?? "Address unavailable"}
                  </Popup>
                </CircleMarker>
              ))}
        </MapContainer>
      </div>
    </div>
  );
}
