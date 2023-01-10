import re
from functools import partial
from typing import List, Callable, Union, Tuple, Dict, Optional

from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from odap.common.config import TIMESTAMP_COLUMN


WindowedColumn = Callable[[str], Column]
TIME_WINDOW_PLACEHOLDER = "time_window"

__HOUR = 60 * 60
__DAY = 24 * __HOUR
__WEEK = 7 * __DAY

PERIODS = {
    "h": __HOUR,
    "d": __DAY,
    "w": __WEEK,
}

PERIOD_NAMES = {
    "s": "SECONDS",
    "m": "MINUTES",
    "h": "HOURS",
    "d": "DAYS",
    "w": "WEEKS",
}


def is_time_window(time_column: Union[str, Column], window_size: int, unit: Optional[str] = None) -> Column:
    unit = unit if unit is not None else "day"
    time_column = f.col(time_column) if isinstance(time_column, str) else time_column

    return time_column.between(f.col("timestamp") - f.lit(window_size).cast(f"interval {unit}"), f.col("timestamp"))


def time_windowed(
    column: Union[str, Column], time_column: Union[str, Column], window_size: int, unit: Optional[str] = None
) -> Column:
    unit = unit if unit is not None else "day"

    return f.when(is_time_window(time_column, window_size, unit), column).otherwise(None)


def get_time_windowed_for_time_column(time_column: Union[str, Column]) -> Callable[[Column, int], Column]:
    def time_windowed_for_time_column(column: Column, num_days: int, unit: Optional[str] = None) -> Column:
        return time_windowed(column, time_column, num_days, unit)

    return time_windowed_for_time_column


# pylint: disable=invalid-name
_time_window_column_template = f"is_time_window_{{{TIME_WINDOW_PLACEHOLDER}}}"


def is_time_window_parsable(time_window: str) -> bool:
    return re.match(f"([0-9]+)({'|'.join(PERIOD_NAMES.keys())})", time_window) is not None


def parse_time_window(time_window: str) -> Dict[str, int]:
    result = {}
    period_name = PERIOD_NAMES[time_window[-1]].lower()
    result[period_name] = int(time_window[:-1])
    return result


def time_window_to_seconds(time_window: str) -> int:
    return int(time_window[:-1]) * PERIODS[time_window[-1]]


def get_max_time_window(time_windows: List[str]) -> Tuple[str, int]:
    result = {time_window: time_window_to_seconds(time_window) for time_window in time_windows}
    return max(result.items(), key=lambda x: x[1])


def is_past_time_window(timestamp: Column, time_column_to_be_subtracted: Column, time_window: str) -> Column:
    period = PERIODS[time_window[-1]] * int(time_window[:-1])
    delta = timestamp - time_column_to_be_subtracted
    return (0 <= delta) & (delta <= period)


def resolve_column_type(df: DataFrame, window_col: str) -> Column:
    dtypes = dict(df.dtypes)

    if window_col not in dtypes:
        raise ValueError(f"Column {window_col} not found in dataframe.")

    dtype = dtypes[window_col]

    if dtype == "date":
        return f.to_timestamp(f.col(window_col)).cast("long")

    if dtype == "timestamp":
        return f.col(window_col).cast("long")

    raise TypeError(f"Column {window_col} is of unsupported type '{dtype}'. Must be either 'date' or 'timestamp'.")


def __with_time_windows(
    df: DataFrame,
    timestamp: str,
    time_column_name: str,
    time_windows: List[str],
    is_time_window_function: Callable,
    time_window_column_template: str,
) -> DataFrame:
    timestamp_col = resolve_column_type(df, timestamp)
    time_column_to_be_subtracted = resolve_column_type(df, time_column_name)

    time_window_columns = [
        is_time_window_function(timestamp_col, time_column_to_be_subtracted, time_window).alias(
            time_window_column_template.format(time_window=time_window)
        )
        for time_window in time_windows
    ]

    return df.select("*", *time_window_columns)


def with_time_windows(df: DataFrame, timestamp: str, time_column_name: str, time_windows: List[str]) -> DataFrame:
    return __with_time_windows(
        df, timestamp, time_column_name, time_windows, is_past_time_window, _time_window_column_template
    )


def windowed(col: Column, time_window: str) -> Column:
    time_window_col_name = _time_window_column_template.format(time_window=time_window)
    return f.when(f.col(time_window_col_name), col).otherwise(None)


def __windowed_col(fun: Callable, cols: List[Column], name: str, time_window: str, metadata: Dict) -> Column:
    return fun(
        *(
            windowed(
                col,
                time_window,
            )
            for col in cols
        )
    ).alias(name, metadata=metadata)


def windowed_column(fun: Callable):
    def wrapper(name: str, cols: Union[Column, List[Column]]):
        cols = cols if isinstance(cols, list) else [cols]
        return partial(__windowed_col, fun, cols, name, metadata={})

    return wrapper


def windowed_column_with_metadata(fun: Callable):
    def wrapper(name: str, cols: Union[Column, List[Column]], metadata: Dict):
        cols = cols if isinstance(cols, list) else [cols]
        return partial(__windowed_col, fun, cols, name, metadata=metadata)

    return wrapper


def sum_windowed(name: str, col: Column) -> WindowedColumn:
    return windowed_column(f.sum)(name, col)


def count_windowed(name: str, col: Column) -> WindowedColumn:
    return windowed_column(f.count)(name, col)


def count_distinct_windowed(name: str, cols: List[Column]) -> WindowedColumn:
    return windowed_column(f.countDistinct)(name, cols)


def min_windowed(name: str, col: Column) -> WindowedColumn:
    return windowed_column(f.min)(name, col)


def max_windowed(name: str, col: Column) -> WindowedColumn:
    return windowed_column(f.max)(name, col)


def mean_windowed(name: str, col: Column) -> WindowedColumn:
    return windowed_column(f.mean)(name, col)


def avg_windowed(name: str, col: Column) -> WindowedColumn:
    return windowed_column(f.avg)(name, col)


def first_windowed(name: str, col: Column) -> WindowedColumn:
    return windowed_column(f.first)(name, col)


def collect_set_windowed(name: str, col: Column) -> WindowedColumn:
    return windowed_column(f.collect_set)(name, col)


def collect_list_windowed(name: str, col: Column) -> WindowedColumn:
    return windowed_column(f.collect_list)(name, col)


class WindowedDataFrame(DataFrame):
    __timestamp_column = TIMESTAMP_COLUMN

    def __init__(self, df: DataFrame, time_column: str, time_windows: List[str]):
        super().__init__(df._jdf, df.sql_ctx)  # noqa # pyre-ignore[6]
        self.__time_column = time_column
        self.__time_windows = time_windows

    def __getattribute__(self, name):
        attr = object.__getattribute__(self, name)
        if hasattr(attr, "__call__"):
            # Always return WindowedDataFrame instead of DataFrame
            def wrapper(*args, **kwargs):
                result = attr(*args, **kwargs)
                if isinstance(result, DataFrame):
                    return WindowedDataFrame(result, self.__time_column, self.__time_windows)

                return result

            return wrapper
        return attr

    def get_windowed_column_list(self, column_names: List[str]) -> List[str]:
        result = []
        for time_window in self.__time_windows:
            result.extend([col.format(time_window=time_window) for col in column_names])
        return result

    def apply_per_time_window(self, fun: Callable[["WindowedDataFrame", str], "WindowedDataFrame"]) -> DataFrame:
        wdf = self
        for time_window in self.__time_windows:
            wdf = fun(wdf, time_window)
        return wdf.df

    def is_time_window(self, time_window: str) -> Column:
        return is_past_time_window(
            resolve_column_type(self.df, self.__timestamp_column),
            resolve_column_type(self.df, self.__time_column),
            time_window,
        )

    def time_windowed(
        self,
        group_keys: List[str],
        agg_columns_function: Callable[[str], List[Union[WindowedColumn, Column]]] = lambda x: [],
        non_agg_columns_function: Callable[[str], List[Column]] = lambda x: [],
        unnest_structs: bool = False,
    ) -> DataFrame:
        self.__check_group_keys(group_keys)

        agg_cols, do_time_windows = self.__get_agg_cols(agg_columns_function)

        df = (
            with_time_windows(
                self.df,
                self.__timestamp_column,
                self.__time_column,
                self.time_windows,
            ).cache()
            if do_time_windows
            else self.df
        )

        grouped_df = (df.groupby(group_keys).agg(*agg_cols)) if agg_cols else df
        non_agg_cols = self.__get_non_agg_cols(non_agg_columns_function)

        return self.__return_dataframe(grouped_df.select("*", *non_agg_cols), unnest_structs)

    @property
    def df(self) -> DataFrame:
        return DataFrame(self._jdf, self.sql_ctx)  # pyre-ignore[6]

    @property
    def timestamp_column(self):
        return self.__timestamp_column

    @property
    def time_column(self):
        return self.__time_column

    @property
    def time_windows(self):
        return self.__time_windows

    def __return_dataframe(self, df: DataFrame, unnest_structs: bool):
        structs = list(map(lambda col: col[0], filter(lambda col: col[1].startswith("struct"), df.dtypes)))
        non_structs = list(map(lambda col: col[0], (filter(lambda col: col[0] not in structs, df.dtypes))))

        return df.select(*non_structs, *map(lambda struct: f"{struct}.*", structs)) if unnest_structs else df

    def __check_group_keys(self, group_keys: List[str]):
        if self.__timestamp_column not in group_keys:
            raise Exception(f"{self.__timestamp_column} missing in group keys")

    # pylint: disable=too-many-statements
    def __get_agg_cols(self, agg_columns_function: Callable[[str], List[Union[WindowedColumn, Column]]]):
        do_time_windows = False

        def resolve_partial(window: str, partial_col: Callable[[str], WindowedColumn]):
            return partial_col(window)

        agg_cols = []
        for time_window in self.time_windows:
            agg_output = agg_columns_function(time_window)

            cols = filter(lambda x: isinstance(x, Column), agg_output)
            partial_cols = filter(lambda x: isinstance(x, partial), agg_output)

            agg_cols.extend(cols)
            agg_cols_len = len(agg_cols)

            resolver = partial(resolve_partial, time_window)
            agg_cols.extend(map(resolver, partial_cols))

            if len(agg_cols) > agg_cols_len:
                do_time_windows = True

        return agg_cols, do_time_windows

    def __get_non_agg_cols(self, non_agg_columns_function: Callable[[str], List[Column]]) -> List[Column]:
        non_agg_cols = []
        for time_window in self.time_windows:
            non_agg_cols.extend(non_agg_columns_function(time_window))
        return non_agg_cols
