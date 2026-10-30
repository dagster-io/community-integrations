from pathlib import Path

import dagster as dg

from dagster_pixeltable import PixeltableResource

VIDEOS_PATH = "dagster_video_demo.videos"
FRAMES_PATH = "dagster_video_demo.frames"
CLIP_PATH = Path(__file__).parent / "clip.mp4"


@dg.asset
def ingest_videos(pixeltable: PixeltableResource) -> dg.MaterializeResult:
    videos = pixeltable.get_table(VIDEOS_PATH)
    # each run appends the clip again: batch_update() cannot write media columns, so there is no upsert
    videos.insert([{"title": "clip", "video": str(CLIP_PATH)}], on_error="abort")
    frames = pixeltable.get_table(FRAMES_PATH)
    return dg.MaterializeResult(
        metadata={
            "dagster/table_name": VIDEOS_PATH,
            "dagster/row_count": videos.count(),
            "pixeltable/version": videos.get_metadata()["version"],
            "pixeltable/frame_count": frames.count(),
        }
    )


defs = dg.Definitions(
    assets=[ingest_videos],
    resources={
        "pixeltable": PixeltableResource(expected_home=dg.EnvVar("PIXELTABLE_HOME"))
    },
    executor=dg.in_process_executor,
)
