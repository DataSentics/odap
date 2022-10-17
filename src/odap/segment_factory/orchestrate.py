from odap.common.config import get_config_namespace, ConfigNamespace
from odap.segment_factory.config import get_segments
from odap.segment_factory.segments import create_segment_dataframe_by_slug, save_segment_dataframe
from odap.segment_factory.exports import run_exports


def orchestrate():
    config = get_config_namespace(ConfigNamespace.SEGMENT_FACTORY)

    segments = get_segments(config)

    for segment_slug in segments.keys():
        segment_df = create_segment_dataframe_by_slug(segment_slug)
        save_segment_dataframe(segment_df, segment_slug, config)

    run_exports()
