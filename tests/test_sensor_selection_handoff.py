"""Selection hands every hotspot-to-station mapping to the UI, beyond the preview."""
from types import SimpleNamespace

import pandas as pd
import pytest

from src.air_quality_workflow_service import AirQualityWorkflowService
from src.orchestration_model import HaversineSelector, SensorCandidate, WorkflowMode


@pytest.mark.parametrize('mode, expected_mappings', [
    ('nearest_k', 13),
    ('radius_km', 0),
])
def test_selected_stations_come_from_complete_mapping(
    tmp_path, mode, expected_mappings,
):
    # Twelve different selected stations plus one repeated station. The last
    # two unique stations would be lost if the UI relied on the ten-row preview.
    candidates = [
        SensorCandidate(sensor_id=f'S{i}', name=f'sensor-{i}', lat=40, lon=-112 + i * .01)
        for i in range(12)
    ]
    hotspots = tmp_path / 'hotspots.csv'
    pd.DataFrame([
        {'lat': 40.001, 'lon': candidate.lon, 'timestamp': '2025-01-16T06:00:00Z'}
        for candidate in candidates + candidates[:1]
    ]).to_csv(hotspots, index=False)
    selector = HaversineSelector()

    def run(*, mode, **kwargs):
        if mode == WorkflowMode.DISCOVER:
            return {'candidate_details': candidates}
        assert mode == WorkflowMode.SELECT
        return {'mapping_rows': selector.build_mappings(
            kwargs['candidates'], kwargs['events'], kwargs['selection_strategy'],
        )}

    # Inject catalog discovery only; run the actual per-hotspot selector and
    # serialization, without downloading forecasts or contacting a catalog.
    service = object.__new__(AirQualityWorkflowService)
    service.workflow = SimpleNamespace(run=run)
    service.map_dir = tmp_path
    service.create_clients = lambda server: ('local', object(), object())
    service.build_public_map_url = lambda name: f'https://example.test/{name}'
    result = service.select_sensors(
        hotspot_path=hotspots,
        selection_request={'selection_mode': mode, 'k': 1, 'radius_km': .01},
    )

    assert result['sensors_found'] == 12
    assert result['mapping_count'] == expected_mappings
    assert len(result['selected_sensors']) == expected_mappings
    assert len(result['preview']) == min(expected_mappings, 10)
    expected_names = [f'sensor-{i}' for i in range(12)] + ['sensor-0'] if expected_mappings else []
    assert [station['sensor_name'] for station in result['selected_sensors']] == expected_names
    if expected_mappings:
        assert result['selected_sensors'][-1]['hotspot_lat'] == 40.001
        assert result['selected_sensors'][-1]['timestamp'] == '2025-01-16T06:00:00Z'
    assert len(pd.read_csv(result['map_csv'])) == expected_mappings
