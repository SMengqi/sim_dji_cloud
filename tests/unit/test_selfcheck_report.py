import json
from pathlib import Path
from sim_dji_cloud.selfcheck.report import write_reports
from sim_dji_cloud.selfcheck.comparator import TopicCompareResult


def test_write_summary_json_and_md(tmp_path: Path):
    results = [
        TopicCompareResult(
            topic="thing/product/SN_DOCK_FIXTURE/osd",
            device_sn="SN_DOCK_FIXTURE",
            orig_count=100, replayed_count=100,
            first_divergence_index=None, first_divergence_field=None,
            max_relative_drift_ms=12,
            out_of_order_count=0, max_reorder_offset=0,
            status="PASS",
        ),
        TopicCompareResult(
            topic="thing/product/SN_DRONE_FIXTURE/osd",
            device_sn="SN_DRONE_FIXTURE",
            orig_count=50, replayed_count=49,
            first_divergence_index=None,
            first_divergence_field="count(orig=50, replay=49)",
            max_relative_drift_ms=0,
            out_of_order_count=0, max_reorder_offset=0,
            status="FAIL",
        ),
    ]
    out_dir = tmp_path / "selfcheck_out"
    write_reports(out_dir, results, tolerance_ms=50, flight_dir_name="T-FIXTURE")

    summary_json = json.loads((out_dir / "summary.json").read_text())
    assert summary_json["overall"] == "FAIL"
    assert summary_json["tolerance_ms"] == 50
    assert len(summary_json["topics"]) == 2

    summary_md = (out_dir / "summary.md").read_text()
    assert "SN_DOCK_FIXTURE" in summary_md
    assert "SN_DRONE_FIXTURE" in summary_md
    assert "PASS" in summary_md
    assert "FAIL" in summary_md


def test_overall_pass_when_all_topics_pass(tmp_path: Path):
    results = [
        TopicCompareResult(
            topic="t", device_sn="s", orig_count=1, replayed_count=1,
            first_divergence_index=None, first_divergence_field=None,
            max_relative_drift_ms=0,
            out_of_order_count=0, max_reorder_offset=0, status="PASS",
        ),
    ]
    out_dir = tmp_path / "out"
    write_reports(out_dir, results, tolerance_ms=50, flight_dir_name="T1")
    assert json.loads((out_dir / "summary.json").read_text())["overall"] == "PASS"
