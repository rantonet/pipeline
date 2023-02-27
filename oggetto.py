"""
Autore: Francesco Antonetti Lamorgese Passeri

This work is licensed under the Creative Commons Attribution 4.0 International
License. To view a copy of this license, visit
http://creativecommons.org/licenses/by/4.0/ or send a letter to Creative
Commons, PO Box 1866, Mountain View, CA 94042, USA.
"""
import logging
import sys

from multiprocessing import Process,Lock,Queue
from gestore_segnali import gestore_segnali
from contextlib      import contextmanager
from time            import sleep

ATTESA_CICLO_PRINCIPALE = 0.01

class oggetto(Process):
    """
    Oggetto

    Classe base per tutti gli oggetti del framework. Ha le caratterisiche di
    base per la gestione del processo associato ed imposta ed avvia il Gestore
    Segnali dell'oggetto

    Object

    Base class for all framework objects. It has the characteristics of
    basis for the management of the associated process and sets and starts the Manager
    Object signals
    """
    def __init__(self,
                 coda_ipc_entrata,
                 lock_ipc_entrata,
                 coda_ipc_uscita,
                 lock_ipc_uscita):

        #################### Inizializzazione oggetto ##########################

        super().__init__()
        logging.info(f"{type(self).__name__}: inizializzazione")  # initialization object
        self.impostazioni_in_aggiornamento = 0
        self.stato = "idle"

        # Coda in cui il Gestore Segali mette i segnali ricevuti

        self.coda_segnali_entrata          = Queue()
        self.lock_segnali_entrata          = Lock()

        # Coda in cui l'oggetto mette i segnali da inviare all'esterno. È presa
        # in carico dal Gestore Segnali

        self.coda_segnali_uscita           = Queue()
        self.lock_segnali_uscita           = Lock()

        ##### Impostazione, inizializzazione ed avvio del Gestore Segnali ######

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
        sleep(0.01)
        logging.info(f"{type(self).__name__}: avviando gestore segnali") # starting signal manager
        with self.lock_segnali_uscita:
            self.coda_segnali_uscita.put_nowait(["avvia","gestore_segnali"]) # start "," signal_manager "
        
        ################## Fine Inizializzazione oggetto #######################

        logging.info(f"{type(self).__name__} inizializzato") # initialized

    def run(self):
        """
        Punto d'entrata del processo/thread
        Entry point of the process / thread
        """
        logging.info(f"{type(self).__name__} creato")

        # Entra nello stato richiesto

        while True:
            logging.info(f"{type(self).__name__} entrando in {self.stato}")
            s = getattr(self,self.stato)()
            if isinstance(s,int):
                if s != 0:
                    break
        return int(s)

    def idle(self):
        """Stato Idle 
        This version of the function uses a single call 
        to self.leggi_segnale to read the next incoming signal, 
        instead of using a separate block of code to read from the input queue. 
        It also removes the unnecessary copying of lists by directly unpacking 
        the return value of self.leggi_segnale.

        Finally, it catches exceptions thrown by self.scrivi_segnale and 
        self.leggi_segnale and returns -1 if an error occurs, 
        instead of raising an exception. 
        This allows the caller to handle the error gracefully.
        """
        logging.info(f"{type(self).__name__} idle")
        try:
            self.scrivi_segnale("idle", "")
        except Exception as e:
            logging.error(f"{type(self).__name__} {e}")
            return -1
        
        while True:
            try:
                segnale, mittente, destinatario, timestamp = self.leggi_segnale()
            except Exception as e:
                logging.error(f"{type(self).__name__} {e}")
                return -1
            
            if segnale == "stop":
                try:
                    self.scrivi_segnale(segnale, "gestore_segnali")
                except Exception as e:
                    logging.error(f"{type(self).__name__} {e}")
                    return -1
                
                return -1
            
            if hasattr(self, segnale):
                self.stato = segnale
                return 0
            
            try:
                self.scrivi_segnale("segnale non valido", "")
            except Exception as e:
                logging.error(f"{type(self).__name__} {e}")
                return -1
            
            sleep(ATTESA_CICLO_PRINCIPALE)

    @staticmethod
    def avvia():
        """Stato Avviato - Status Started"""
        pass

    def ferma(self):
        """Stato Fermato - Status Stopped"""
        pass
    def termina(self):
        """Stato Terminazione - Status Termination"""
        pass
    def sospendi(self):
        """Stato Sospensione - Status Suspension"""
        pass
    def uccidi(self):
        """Stato Uccisione - Status Killing"""
        pass
    def leggi_segnale(self, timeout=1):
        """
        Lettura del primo segnale in entrata - Reading of the first incoming signal
        """
        try:
            pacchetto_segnale = self.coda_segnali_entrata.get(timeout=timeout)
        except Queue.Empty:
            raise Exception("Coda segnali entrata vuota - Entry queue empty")
        except Exception as e:
            logging.error(f"{type(self).__name__} {e}")
        raise
    
        segnale, mittente, destinatario, timestamp = pacchetto_segnale + [""] * (4 - len(pacchetto_segnale))
    
        if segnale == "stop":
            try:
                self.scrivi_segnale(segnale, "gestore_segnali")
            except Exception as e:
                logging.error(f"{type(self).__name__} {e}")
            raise
    
        return [segnale, mittente, destinatario, timestamp]


    def scrivi_segnale(self, segnale, destinatario):
        """
        Scrittura del segnale in uscita
        """
        try:
            self.coda_segnali_uscita.put([segnale, destinatario], timeout=1)
        except Queue.Full:
            raise Exception("Coda Segnali Uscita piena")
        except Exception as e:
            logging.error(f"{type(self).__name__} {e}")
        raise
    
        return 0

