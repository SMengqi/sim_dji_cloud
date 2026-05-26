from sim_dji_cloud.recorder.video_writer import resolve_video_source_url


def test_returns_override():
    assert resolve_video_source_url(
        {"source_url_override": "rtmp://srs/live/x"}) == "rtmp://srs/live/x"


def test_strips_whitespace():
    assert resolve_video_source_url(
        {"source_url_override": "  rtmp://srs/live/x  "}) == "rtmp://srs/live/x"


def test_empty_or_missing_is_none():
    assert resolve_video_source_url({"source_url_override": ""}) is None
    assert resolve_video_source_url({"source_url_override": "   "}) is None
    assert resolve_video_source_url({"source_url_override": None}) is None
    assert resolve_video_source_url({}) is None
