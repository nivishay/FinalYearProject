from abc import ABC, abstractmethod

class dbUploader(ABC):
    """
    Abstract base class for uploading a MySQL dump to any cloud provider.
    """
    def __init__(self):
        pass

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def upload(self, dump_path):
        pass

    @abstractmethod
    def close(self):
        pass
