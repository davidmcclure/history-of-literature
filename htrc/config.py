

import os
import anyconfig

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager


class Config:


    @classmethod
    def from_env(cls):

        """
        Get a config instance with the default files.
        """

        return cls([
            os.path.join(os.path.dirname(__file__), 'htrc.yml'),
            '~/.htrc.yml',
            '/etc/htrc.yml',
        ])


    def __init__(self, paths):

        """
        Initialize the configuration object.

        Args:
            paths (list): YAML paths, from most to least specific.
        """

        self.paths = paths
        self.read()


    def __getitem__(self, key):

        """
        Get a configuration value.

        Args:
            key (str): The configuration key.

        Returns:
            The option value.
        """

        return self.config.get(key)


    def read(self):

        """
        Load the configuration files, set connections.
        """

        self.config = anyconfig.load(self.paths, ignore_missing=True)

        self.session = self.make_session()


    def make_engine(self):

        """
        Build a SQLAlchemy engine.

        Returns: Engine
        """

        return create_engine(self['database'])


    def make_session(self):

        """
        Build a SQLAlchemy session class.

        Returns: Session
        """

        Session = sessionmaker(bind=self.make_engine())

        return Session()
