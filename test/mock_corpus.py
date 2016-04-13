

import os
import tempfile
import json
import shutil

from bz2 import compress


class MockCorpus:


    def __init__(self):

        """
        Create the temporary directory.
        """

        self.path = tempfile.mkdtemp()


    def teardown(self):

        """
        Delete the temporary directory.
        """

        shutil.rmtree(self.path)


    def add_vol(self, vol):

        """
        Add a volume to the corpus.

        Args:
            vol (Volume)
        """

        path = os.path.join(self.path, '{0}.json.bz2'.format(vol.id))

        data = compress(json.dumps(vol.data).encode('utf8'))

        with open(path, 'wb') as fh:
            fh.write(data)
