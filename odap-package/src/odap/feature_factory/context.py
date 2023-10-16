from odap.feature_factory.config import get_feature_factory_config
from odap.feature_factory.dataframes.dataframe_writer import get_table_df_mapping
from odap.feature_factory.feature_notebook import get_feature_notebooks_from_dirs, create_notebook_table_mapping


class Context:
    def __init__(self):
        self.config = get_feature_factory_config()
        self.feature_notebooks = get_feature_notebooks_from_dirs(self.config)
        self.notebook_table_mapping = create_notebook_table_mapping(self.feature_notebooks)
        self.table_df_mapping = get_table_df_mapping(self.notebook_table_mapping, self.config)
