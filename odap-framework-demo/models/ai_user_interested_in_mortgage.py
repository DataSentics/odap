from pyspark.sql import functions as f
from pyspark.ml.pipeline import Transformer


class AiUserInterestedInMortgageModel(Transformer):
    def __init__(self):
        super(AiUserInterestedInMortgageModel, self).__init__()

    def _transform(self, dataset):
        return dataset.withColumn("ai_user_interested_in_mortgage_estimation", f.rand(10))
