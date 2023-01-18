from typing import List
from pyspark.sql import functions as f
from odap.feature_factory import time_windows as tw

products = ["investice", "pujcky", "hypoteky"]

def product_agg_features(time_window: str) -> List[tw.WindowedColumn]:
    return [
        tw.sum_windowed(
            f"{product}_web_visits_count_in_last_{time_window}",
            f.lower("url").contains(product).cast("integer"),
        )
        for product in products
    ]
