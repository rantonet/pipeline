from multiprocessing import Process,Lock,Queue
from gestore_segnali import gestore_segnali
from time            import sleep,time

class oggetto(Process):
    def __init__(self,
                 coda_ipc_entrata,
                 lock_ipc_entrata,
                 coda_ipc_uscita,
                 lock_ipc_uscita):
        #################### Inizializzazione oggetto ##########################
        super().__init__()
        # Coda in cui il Gestore Segali mette i segnali ricevuti
        self.coda_segnali_entrata = Queue()
        self.lock_segnali_entrata = Lock()

        # Coda in cui l'oggetto mette i segnali da inviare all'esterno. È presa
        # in carico dal Gestore Segnali
        self.coda_segnali_uscita  = Queue()
        self.lock_segnali_uscita  = Lock()

        ######### Impostazione ed inizializzazione del Gestore Segnali #########
        self.gestore_segnali      = gestore_segnali(type(self).__name__,
                                                    coda_ipc_entrata,
                                                    lock_ipc_entrata,
                                                    coda_ipc_uscita,
                                                    lock_ipc_uscita,
                                                    self.coda_segnali_entrata,
                                                    self.lock_segnali_entrata,
                                                    self.coda_segnali_uscita,
                                                    self.lock_segnali_uscita)
        self.gestore_segnali.start()
        with lock_ipc_uscita:
            coda_ipc_uscita.put_nowait("avvia:" + str(time()) + ":" + \
                                                      type(self).__name__ + ":")
        ################## Fine Inizializzazione oggetto #######################
    def run(self):
        self.idle()
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
