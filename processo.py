from multiprocessing import Process

class processo(Process):
    def __init__(self,coda_ipc,lock_ipc):
        super().__init__()
        self.ipc = coda_ipc
        self.lock_ipc = lock_ipc
    def idle(self):
        pass
    def avvia(self):
        pass
    def ferma(self):
        pass
    def termina(self):
        pass
    def sospendi(self):
        pass
    def uccidi(self):
        pass