class NotebookException(Exception):
    def __init__(self, message, feature):
        message = f"{message} At Feature: {feature}"
        super().__init__(message)


class WrongFillnaValueTypeError(Exception):
    def __init__(self, value: str, name_template: str, dtype: str):
        message = f"Value for fillna_with: {value} is not compatible with feature {name_template} of the type {dtype}"
        super().__init__(message)
