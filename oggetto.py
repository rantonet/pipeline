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
        ##################### Object initialization ###########################
        super().__init__()
        logging.info("oggetto inizializzazione") # initialization object
        self.impostazioni_in_aggiornamento = 0
        self.stato                         = "idle"
        # Coda in cui il Gestore Segali mette i segnali ricevuti
        # Queue where the Signal Manager puts the received signals
        self.coda_segnali_entrata          = Queue()
        self.lock_segnali_entrata          = Lock()
        # Coda in cui l'oggetto mette i segnali da inviare all'esterno. È presa
        # in carico dal Gestore Segnali
        # Queue where the object puts the signals to send out. It is taken
        # charged by the Signal Manager
        self.coda_segnali_uscita           = Queue()
        self.lock_segnali_uscita           = Lock()
        ##### Impostazione, inizializzazione ed avvio del Gestore Segnali ######
        ##### Setting, initializing and starting the Signal Manager ######
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
        logging.info(str(type(self).__name__) + ": avviando gestore segnali") # starting signal manager
        with self.lock_segnali_uscita:
            self.coda_segnali_uscita.put_nowait(["avvia","gestore_segnali"]) # start "," signal_manager "
        ################## Fine Inizializzazione oggetto #######################
        ################### End Object initialization ########################
        logging.info(type(self).__name__ + " inizializzato") # initialized
    def run(self):
        """
        Punto d'entrata del processo/thread
        Entry point of the process / thread
        """
        logging.info(type(self).__name__ + " creato") # created
        # Entra nello stato richiesto
        # Enter the required state
        while True:
            logging.info(type(self).__name__ + " entrando in " + self.stato) # entering
            s = getattr(self,self.stato)()
            if isinstance(s,int):
                if s != 0:
                    break
        return int(s)
    def idle(self):
        """
        Stato Idle

        Idle state
        """

        logging.info(type(self).__name__ + " idle")

        pacchetto_segnale_entrata = []
        segnale                   = ""
        mittente                  = ""
        destinatario              = ""
        timestamp                 = 0

        # with self.lock_segnali_uscita:
        #     if not self.coda_segnali_uscita.full():
        #         self.coda_segnali_uscita.put_nowait(["idle",""])

        try:
            self.scrivi_segnale("idle","")
        except:
            e = sys.exc_info()[0]
            logging.info(type(self).__name__ + str(e))

        while True:
            pacchetto_segnale_entrata[:] = []
            segnale                      = ""
            mittente                     = ""
            destinatario                 = ""
            timestamp                    = 0

            # with self.lock_segnali_entrata:
            #     if not self.coda_segnali_entrata.empty():
            #         pacchetto_segnale_entrata[:] = self.coda_segnali_entrata.get_nowait()
            # if len(pacchetto_segnale_entrata) == 4:
            #     segnale,mittente,destinatario,timestamp = pacchetto_segnale_entrata
            #     pacchetto_segnale_entrata[:] = []
            # elif len(pacchetto_segnale_entrata) == 3:
            #     segnale,mittente,timestamp = pacchetto_segnale_entrata
            #     pacchetto_segnale_entrata[:] = []
            # elif len(pacchetto_segnale_entrata) == 0:
            #     pass
            # else:
            #     with self.lock_segnali_uscita:
            #         if not self.coda_segnali_uscita.full():
            #             self.coda_segnali_uscita.put_nowait(["segnale mal formato",""])
            #     pacchetto_segnale_entrata[:] = []
            #     sleep(ATTESA_CICLO_PRINCIPALE)
            #     continue

            try:
                segnale,mittente,destinatario,timestamp = self.leggi_segnale()
            except:
                e = sys.exc_info()[0]
                logging.info(type(self).__name__ + str(e))

            if segnale == "stop":
                # with self.lock_segnali_uscita:
                #     if not self.coda_segnali_uscita.full():
                #         self.coda_segnali_uscita.put_nowait(["stop","gestore_segnali"])

                try:
                    self.scrivi_segnale(segnale,"gestore_segnali")
                    return int(-1)
                except:
                    e = sys.exc_info()[0]
                    logging.info(type(self).__name__ + str(e))
            else:
                if segnale in dir(self):
                    self.stato = segnale
                    return 0
                else:
                    # with self.lock_segnali_uscita:
                    #     if not self.coda_segnali_uscita.full():
                    #         self.coda_segnali_uscita.put_nowait(["segnale non valido",""])

                    try:
                        self.scrivi_segnale("segnale non valido","")
                        return int(-1)
                    except:
                        e = sys.exc_info()[0]
                        logging.info(type(self).__name__ + str(e))

            sleep(ATTESA_CICLO_PRINCIPALE)
    def avvia(self):
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
    def leggi_segnale(self):
        """
        Lettura del primo segnale in entrata
        Reading of the first incoming signal
        """

        pacchetto_segnale = []
        segnale           = ""
        mittente          = ""
        destinatario      = ""
        timestamp         = 0

        with self.coda_segnali_entrata:
            if not self.coda_segnali_entrata.empty():
                try:
                    pacchetto_segnale[:] = self.coda_segnali_entrata.get_nowait()
                except:
                    e = sys.exc_info()[0]
                    logging.error(type(self).__name__ + str(e))
                    raise
            else:
                raise "Coda segnali entrata vuota" # Entry queue empty
        if len(pacchetto_segnale) == 4:
            segnale,mittente,destinatario,timestamp = pacchetto_segnale
        elif len(pacchetto_segnale) == 3:
            segnale,mittente,timestamp = pacchetto_segnale
        elif len(pacchetto_segnale) == 0:
            raise "Nessun sengale" # No signal
        else:
            raise "Segnale mal formato" # Badly formed signal
        if segnale == "":
            raise "Segnale vuoto" # Empty signal
        elif segnale == "stop":
            # with self.lock_segnali_uscita:
            #     if not self.coda_segnali_uscita.full():
            #         try:
            #             self.coda_segnali_uscita.put_nowait(["stop","gestore_segnali"])
            #         except:
            #             e = sys.exc_info()[0]
            #             logging.error(type(self).__name__ + str(e))
            #     else:
            #         raise "Coda Segnali Uscita piena"
            
            try:
                self.scrivi_segnale(["stop","gestore_segnali"]) # stop "," signal_manager
            except:
                e = sys.exc_info()[0]
                logging.error(type(self).__name__ + str(e))
                raise
        return [segnale,mittente,destinatario,timestamp]
    def scrivi_segnale(self,segnale,destinatario):
        """Lettura del segnale in uscita"""
        stato = 0
        with self.lock_segnali_uscita:
            if not self.coda_segnali_uscita.full():
                try:
                    self.coda_segnali_uscita.put_nowait([segnale,destinatario])
                except:
                    e = sys.exc_info()[0]
                    logging.error(type(self).__name__ + str(e))
                    raise
            else:
                raise "Coda Segnali Uscita piena - Signal Queue Output full"
        return stato
