# ruff: noqa: F821  # a model body refers to its own columns by bare name, which type checkers reject

import pixeltable as pxt
import pixeltable.functions as pxtf

TableModel = pxt.model_base()


class Documents(TableModel, name="documents"):
    source_id = pxt.Column(type=pxt.String, primary_key=True)
    body: pxt.String
    page_count: pxt.Int
    body_upper = pxtf.string.upper(body)  # type: ignore
    # a record with page_count 0 stores a division error in this cell instead of failing the write
    chars_per_page = pxtf.string.len(body) / page_count  # type: ignore
