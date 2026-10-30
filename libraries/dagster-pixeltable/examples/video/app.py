# ruff: noqa: F821  # a model body refers to its own and its iterator's columns by bare name, which type checkers reject

import pixeltable as pxt
from pixeltable.functions.video import frame_iterator

TableModel = pxt.model_base()


class Videos(TableModel, name="videos"):
    title: pxt.String
    video: pxt.Video


class Frames(
    TableModel, name="frames", base=Videos, iterator=frame_iterator(video=Videos.video)
):
    thumbnail = frame.resize(size=(32, 32))  # type: ignore
