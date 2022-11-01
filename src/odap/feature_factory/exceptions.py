class NotebookException(Exception):
    def __init__(self, message, feature):
        message = f"{message} At Feature: {feature}"
        super().__init__(message)
