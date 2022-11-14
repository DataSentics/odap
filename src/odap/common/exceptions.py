class ConfigAttributeMissingException(Exception):
    pass


class InvalidConfigAttributException(Exception):
    pass


class NotebookException(Exception):
    def __init__(self, message, path):
        message = f"{message} At: {path}"
        super().__init__(message)


class InvalidNotebookLanguageException(Exception):
    pass
