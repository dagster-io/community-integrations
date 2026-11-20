from collections.abc import Iterator

import pixeltable as pxt
import pytest

from dagster_pixeltable_tests.utils import (
    EXAMPLES_DIR,
    deploy_example,
    load_example,
    run_command,
)

video = load_example("video")


@pytest.fixture
def videos() -> Iterator[pxt.Table]:
    deploy_example("video", "dagster_video_demo")
    table = pxt.get_table(video.VIDEOS_PATH)
    assert table is not None
    yield table
    pxt.drop_dir("dagster_video_demo", force=True)


def test_each_run_appends_the_clip_and_its_frames(videos: pxt.Table) -> None:
    job = video.defs.get_repository_def().get_implicit_global_asset_job_def()
    assert job.executor_def.name == "in_process"
    frames = pxt.get_table(video.FRAMES_PATH)
    assert frames is not None

    frame_counts = []
    for runs in (1, 2):
        result = job.execute_in_process()
        assert result.success
        [event] = result.asset_materializations_for_node("ingest_videos")
        assert event.metadata["dagster/row_count"].value == runs
        frame_counts.append(event.metadata["pixeltable/frame_count"].value)
    assert videos.count() == 2
    assert frame_counts[0] > 0
    assert frame_counts[1] == frames.count() == 2 * frame_counts[0]
    [row] = frames.select(frames.thumbnail).limit(1).collect()
    assert row["thumbnail"].size == (32, 32)


def test_readme_command(videos: pxt.Table) -> None:
    result = run_command(
        "dagster",
        "asset",
        "materialize",
        "-f",
        "video/definitions.py",
        "-a",
        "defs",
        "--select",
        "ingest_videos",
        cwd=EXAMPLES_DIR,
    )
    assert "RUN_SUCCESS" in result.stdout + result.stderr
    assert videos.count() == 1


def test_batch_update_rejects_media_even_for_new_rows(catalog_dir: str) -> None:
    table = pxt.create_table(
        f"{catalog_dir}.keyed_videos",
        {"video_id": pxt.String, "video": pxt.Video},
        primary_key="video_id",
    )
    with pytest.raises(pxt.Error, match="media column"):
        table.batch_update(
            [{"video_id": "clip", "video": str(video.CLIP_PATH)}],
            if_not_exists="insert",
        )
    assert table.count() == 0
