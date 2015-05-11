
from cresis.inventory import api



class Worker(object):
    def __init__(self, sandbox_dir):
        self._sanbox = sandbox_dir

    @property
    def sandbox_dir(self):
        return self._sanbox

    

    def run(self, runnable):
        pass
