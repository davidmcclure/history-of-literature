

import os
import anyconfig

from wordfreq import top_n_list


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
        Load the configuration files, set globals.
        """

        # Parse the configuration.
        self.config = anyconfig.load(self.paths, ignore_missing=True)

        # Canonical set of tokens.
        self.tokens = self.get_tokens()


    def get_tokens(self):

        """
        Get a set of whitelisted tokens.

        Returns: set
        """

        tokens = top_n_list('en', self['token_depth'], ascii_only=True)

        return set(tokens)
