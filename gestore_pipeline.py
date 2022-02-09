"""
Autore: Francesco Antonetti Lamorgese Passeri

This work is licensed under the Creative Commons Attribution 4.0 International
License. To view a copy of this license, visit
http://creativecommons.org/licenses/by/4.0/ or send a letter to Creative
Commons, PO Box 1866, Mountain View, CA 94042, USA.
"""

import logging

from multiprocessing import Queue,Lock
from time            import time,sleep
from importlib       import import_module

#Framework
from oggetto         import oggetto
from gestore_segnali import gestore_segnali

ATTESA_CICLO_PRINCIPALE = 0.001

class gestore_pipeline(oggetto):
    """Gestore Pipeline

    Gestisce la coda delle operazioni che il programma deve eseguire.
    Fa da arbitro nelle comunicazioni tra le operazioni e tra le operazioni e
    il Gestore Pipeline (sé stesso) ed orchestra le operazioni.
    Si assicura che le operazioni vengano eseguite nell'ordine stabilito.

    Manages the queue of operations that the program must perform.
    Arbitrator in communications between transactions and between transactions e
    the Pipeline Manager (himself) and orchestrates the operations.
    It ensures that the operations are carried out in the established order
    """
    def __init__(self,
                 file_configurazione,
                 coda_ipc_entrata,
                 lock_ipc_entrata,
                 coda_ipc_uscita,
                 lock_ipc_uscita):
        super().__init__(coda_ipc_entrata,
                         lock_ipc_entrata,
                         coda_ipc_uscita,
                         lock_ipc_uscita)
        logging.info(type(self).__name__ + " inizializzazione")

        ##### Inizializzazione comune a tutti gli oggetti del framework ########
        ##### Common initialization for all framework objects ##################

        ##################### Lettura delle impostazioni #######################
        ##################### Reading the settings #############################
        configurazione       = []
        lista_configurazione = []
        impostazioni         = []

        # Leggi le impostazioni dal file configurazione e mettile in una lista #
        # Read the settings from the configuration file and put them in a list #
        with open(file_configurazione) as f:
            configurazione = f.readlines()
        lista_configurazione[:] = [x.strip() for x in configurazione]

        # La lista delle impostazioni è una lista di liste, così da permettere
        # indici non unici
        # The list of settings is a list of lists, so to allow non-unique indices
        for impostazione in lista_configurazione:
            nome,valore = impostazione.split(" ")
            impostazioni.append([nome,valore])
        ################# Fine lettura delle impostazioni ######################
        #### Fine inizializzazione comune a tutti gli oggetti del framework ####
        ################# End of reading the settings ##########################
        ######## End of initialization common to all framework objects #########

        ################### Inizializza le impostazioni ########################
        #################### Initialize the settings #########################

        # TODO: controlla le impostazioni già scritte e inizializza le
        #       impostazioni mancanti
        # TODO: check the settings already written and initialize the
        #       missing settings

        self.lista_segnali                   = []
        # Dizionario con le operazioni da eseguire nell'ordine di esecuzione
        # Dictionary with operations to be performed in order of execution
        self.operazioni                      = {} # "nome": operazione - # "name": operation
        # Dizionario con le code per le comunicazioni tra il Gestore Pipeline e
        # le operazioni. Le code sono unidirezionali: qui ci sono i messaggi che
        # devono essere mandati alle operazioni.
        # Usato *solo* dai Gestori Segnali per le comunicazioni oggetto-oggetto
        # (Gestore Segnali - Gestore Segnali)
        # Dictionary with queues for communications between the Pipeline Manager and
        # the operations. Queues are one-way - here are the messages that
        # must be sent to operations.
        # Used * only * by Signal Handlers for object-to-object communication
        # (Signals Manager - Signals Manager)
        self.ipc_entrata_operazioni          = {} # "nome operazione": coda - # "operation name": queue
        # Dizionario con i lock per le code IPC in entrata
        # Dictionary with locks for incoming IPC queues
        self.lock_ipc_entrata_operazioni     = {} # "nome operazione": coda - # "operation name": queue
        # Dizionario con le code per le comunicazioni tra il Gestore Pipeline e
        # le operazioni. Le code sono unidirezionali: qui ci sono i messaggi
        # ricevuti dalle operazioni.
        # Usato *solo* dai Gestori Segnali per le comunicazioni oggetto-oggetto
        # (Gestore Segnali - Gestore Segnali)
        # Dictionary with queues for communications between the Pipeline Manager and
        # the operations. Queues are one-way - here are the messages
        # received from operations.
        # Used * only * by Signal Handlers for object-to-object communication
        # (Signals Manager - Signals Manager)
        self.ipc_uscita_operazioni           = {}
        # Dizionario con i lock per le code IPC in uscita
        # Dictionary with outbound IPC queue locks
        self.lock_ipc_uscita_operazioni      = {}
        # Dizionario con le code per i segnali ricevuti dalle operazioni. Il
        # Gestore Pipeline legge i segnali delle operazioni da qui
        # Dictionary with queues for signals received by operations. The
        # Pipeline Manager reads the trade signals from here
        self.coda_segnali_entrata_operazioni = {}
        # dizionario con i lock per le code in uscita
        # dictionary with outbound queue locks
        self.lock_segnali_uscita_operazioni  = {}
        # Dizionario con le code per i segnali da inviare alle operazioni. Il
        # Gestore Pipeline scrive i segnali per le operazioni da qui
        # Dictionary with queues for signals to be sent to operations. The
        # Pipeline manager writes signals for operations from here
        self.coda_segnali_uscita_operazioni  = {}
        # Dizionario con i lock per le code in entrata
        # Dictionary with inbound queue locks
        self.lock_segnali_entrata_operazioni = {}
        # Dizionario con i gestori segnali associati alle singole operazioni.
        # Per ogni operazione della pipeline, il Gestore Pipeline si crea un
        # Gestore Segnali per la comunicazione con quella operazione
        # Dictionary with signal handlers associated with single operations.
        # For each pipeline operation, the Pipeline Manager creates a
        # Signal Manager for communicating with that operation
        self.gestore_segnali_operazioni      = {}
        # Segnale in entrata dall'esterno dell'applicazione (dalla coda IPC)

        # Preleva le impostazioni del Gestore Pipeline. Le impostazioni sono:
        # -) Operazione: il nome dell'operazione da aggiungere alla pipeline
        # -) Segnale: un segnale che il Gestore Pipeline può inviare

        # Incoming signal from outside the application (from the IPC queue)

         # Get Pipeline Manager settings. The settings are:
         # -) Operation: the name of the operation to add to the pipeline
         # -) Signal: a signal that the Pipeline Manager can send
        for impostazione in impostazioni:
            nome,valore = impostazione
            # Aggiungi il segnale alla lista dei segnali
            # Add the signal to the signal list
            if nome == "segnale":
                self.lista_segnali.append(valore)
            # Aggiungi l'operazione alla pipeline
            # Add the operation to the pipeline
            if nome == "operazione":
                # Inizializza le code e i lock *associati* all'operazione nel
                # Gestore Pipeline
                # Initialize the queues and locks * associated * with the operation in the
                # Pipeline manager

                # globals()[valore] = getattr(__import__(valore),valore)
                globals()[valore] = getattr(import_module(valore),valore)

                self.ipc_entrata_operazioni[valore]          = Queue()
                self.lock_ipc_entrata_operazioni[valore]     = Lock()
                self.ipc_uscita_operazioni[valore]           = Queue()
                self.lock_ipc_uscita_operazioni[valore]      = Lock()
                self.coda_segnali_entrata_operazioni[valore] = Queue()
                self.lock_segnali_entrata_operazioni[valore] = Lock()
                self.coda_segnali_uscita_operazioni[valore]  = Queue()
                self.lock_segnali_uscita_operazioni[valore]  = Lock()
                # Inizializza il Gestore Segnali *associato* all'operazione
                # Initialize the Signal Manager * associated * with the operation
                self.gestore_segnali_operazioni[valore]      = gestore_segnali(
                                   type(self).__name__,
                                   self.ipc_entrata_operazioni[valore],
                                   self.lock_ipc_entrata_operazioni[valore],
                                   self.ipc_uscita_operazioni[valore],
                                   self.lock_ipc_uscita_operazioni[valore],
                                   self.coda_segnali_entrata_operazioni[valore],
                                   self.lock_segnali_entrata_operazioni[valore],
                                   self.coda_segnali_uscita_operazioni[valore],
                                   self.lock_segnali_uscita_operazioni[valore],
                                   controlla_destinatario=False,
                                   inoltra=True)
                # Avvia il Gestore Segnali *associato* all'operazione
                # Start the Signal Manager * associated * with the operation
                self.gestore_segnali_operazioni[valore].start()
                sleep(0.01)
                with self.lock_segnali_uscita_operazioni[valore]:
                    if not self.coda_segnali_uscita_operazioni[valore].full():
                        self.coda_segnali_uscita_operazioni[valore].put_nowait(["avvia","gestore_segnali"])
                # Inizializza l'operazione nella coda delle operazioni
                # Initialize the operation in the operation queue

                self.operazioni[valore] = globals()[valore](
                                       str(valore + ".conf"),
                                       self.ipc_uscita_operazioni[valore],
                                       self.lock_ipc_uscita_operazioni[valore],
                                       self.ipc_entrata_operazioni[valore],
                                       self.lock_ipc_entrata_operazioni[valore])
                logging.info(self.operazioni[valore])
                sleep(0.1)
        # Avvia tutte le operazioni
        # Start all operations
        for nome,operazione in self.operazioni.items():
            logging.info(type(self).__name__ + " sta avviando " + nome)
            operazione.start()
        ################ Fine inizializza le impostazioni ######################
        ################ Finish initializes the settings #######################
        logging.info(type(self).__name__ + " inizializzato")
    def run(self):
        """Punto d'entrata del processo/thread"""
        logging.info(type(self).__name__ + " creato")
        # Entra nello stato richiesto
        # Enter the required state
        while True:
            logging.info(type(self).__name__ + " entrando in " + self.stato)
            s = getattr(self,self.stato)()
            if isinstance(s,int):
                if s != 0:
                    break
        return int(s)
    def idle(self):
        logging.info(type(self).__name__ + " idle")

        pacchetto_segnale_entrata = []
        segnale                   = ""
        mittente                  = ""
        destinatario              = ""
        timestamp                 = 0

        # Segnala all'esterno che sei in idle
        # It signals to the outside that you are idle
        with self.lock_segnali_uscita:
            if not self.coda_segnali_uscita.full():
                self.coda_segnali_uscita.put_nowait(["idle",""])
        # Attendi il segnale di avvio
        # Wait for the start signal
        while True:
            pacchetto_segnale_entrata[:] = []
            segnale                      = ""
            mittente                     = ""
            destinatario                 = ""
            timestamp                    = 0

            with self.lock_segnali_entrata:
                if not self.coda_segnali_entrata.empty():
                    pacchetto_segnale_entrata[:] = \
                                          self.coda_segnali_entrata.get_nowait()
            if len(pacchetto_segnale_entrata) == 4:
                segnale,mittente,destinatario,timestamp = \
                                                       pacchetto_segnale_entrata
                logging.info(type(self).__name__)
                logging.info(pacchetto_segnale_entrata)
                logging.info(segnale)
            elif len(pacchetto_segnale_entrata) == 3:
                segnale,mittente,timestamp = pacchetto_segnale_entrata
                logging.info(type(self).__name__)
                logging.info(pacchetto_segnale_entrata)
                logging.info(segnale)
            elif len(pacchetto_segnale_entrata) == 0:
                pass
            else:
                with self.lock_segnali_uscita:
                    self.coda_segnali_uscita.put_nowait(["segnale mal formato", # badly formed signal
                                                         ""])
                sleep(0.1)
                logging.info("Gestore Pipeline: Segnale mal formato") # Pipeline Manager: Badly formed signal
                pacchetto_segnale_entrata[:] = []
                continue
            pacchetto_segnale_entrata[:] = []
            if segnale == "":
                sleep(0.01)
                continue
            # Se hai ricevuto il segnale di stop
            elif segnale == "stop":
                with self.lock_segnali_uscita:
                    self.coda_segnali_uscita.put_nowait(["terminato",""]) # finished
                    # Invia il segnale di stop anche al tuo Gestore Segnali
                    # Send the stop signal to your Signal Manager as well
                    self.coda_segnali_uscita.put_nowait(["stop",
                                                         "gestore_segnali"]) # signal_manager
                    # Termina segnalando l'uscita per segnale di stop
                return int(-1)
            else:
                # Se il segnale è tra i metodi riconosciuti dal Gestore Pipeline
                if segnale in dir(self):
                    #Esegui il segnale
                    s = getattr(self,segnale)()
                    return int(s)
                else:
                    with self.lock_segnali_uscita:
                        self.coda_segnali_uscita.put_nowait( \
                                                          ["Segnale non valido", # Invalid signal
                                                           ""])
                    sleep(0.01)
            ############## Fine ricezione messaggi dall'esterno ################
            ############## End of receiving messages from the outside #################
    def avvia(self):
        logging.info(type(self).__name__ + " avviato")

        pacchetto_segnale_entrata = []
        segnale                   = ""
        mittente                  = ""
        destinatario              = ""
        timestamp                 = 0
        richiesta_stop            = False

        # Segnala all'esterno che sei avviato
        # Signals externally that you are running
        with self.lock_segnali_uscita:
            if not self.coda_segnali_uscita.full():
                self.coda_segnali_uscita.put_nowait(["avviato",""]) # started

        for nome,operazione in self.operazioni.items():
            # Manda il segnale di avvio all'operazione.
            # Send the operation start signal.
            with self.lock_ipc_uscita_operazioni[nome]:
                self.ipc_uscita_operazioni[nome].put_nowait("avvia:"  + \
                                                           str(time()) + ":" + \
                                                           type(self).__name__ \
                                                           + ":" + nome) # start
        with self.lock_segnali_uscita:
            if not self.coda_segnali_uscita.full():
                self.coda_segnali_uscita.put_nowait(["pronto",""]) # ready

        while True:
            pacchetto_segnale_entrata[:] = []
            segnale                      = ""
            mittente                     = ""
            destinatario                 = ""
            timestamp                    = 0

            if richiesta_stop:
                for operazione in self.operazioni:
                    with self.lock_segnali_uscita:
                        self.coda_segnali_uscita.put_nowait(["terminando: " + \
                                                             str(operazione),
                                                             ""]) # ending
                    with self.lock_segnali_uscita_operazioni[operazione]:
                        self.coda_segnali_uscita_operazioni[operazione].put_nowait(["stop",operazione])
                    with self.lock_segnali_uscita_operazioni[operazione]:
                        self.coda_segnali_uscita_operazioni[operazione].put_nowait(["stop","gestore_segnali"]) # "stop", "signal_manager"
                    #self.operazioni[operazione].join()
                    with self.lock_segnali_uscita:
                        self.coda_segnali_uscita.put_nowait([str(operazione) + \
                                                             " terminata",""]) # finished
                with self.lock_segnali_uscita:
                    self.coda_segnali_uscita.put_nowait(["stop",
                                                         "gestore_segnali"]) # "stop","signal_manager"]
                return int(-1)

            with self.lock_segnali_entrata:
                if not self.coda_segnali_entrata.empty():
                    pacchetto_segnale_entrata[:] = \
                                          self.coda_segnali_entrata.get_nowait()
            logging.debug("IPC")
            logging.debug(pacchetto_segnale_entrata)
            if len(pacchetto_segnale_entrata) == 4:
                segnale,mittente,destinatario,timestamp = \
                                                       pacchetto_segnale_entrata
                pacchetto_segnale_entrata[:] = []
            elif len(pacchetto_segnale_entrata) == 3:
                segnale,mittente,timestamp = pacchetto_segnale_entrata
                pacchetto_segnale_entrata[:] = []
            elif len(pacchetto_segnale_entrata) == 0:
                pass
            else:
                with self.lock_segnali_uscita:
                    self.coda_segnali_uscita.put_nowait(["segnale mal formato", # badly formed signal
                                                         ""])
                logging.info("Gestore Pipeline: Segnale mal formato") # Pipeline Manager: Badly formed signal
                pacchetto_segnale_entrata[:] = []
                sleep(0.1)
                continue

            # Se hai ricevuto il segnale di stop
            if segnale == "stop":
                # Invia il segnale di stop anche al tuo Gestore Segnali
                with self.lock_segnali_uscita:
                    self.coda_segnali_uscita.put_nowait( \
                                                        ["terminando: " + \
                                                            type(self).__name__,
                                                         ""]) # ending
                richiesta_stop = True
            else:
                if destinatario == "":
                    for (ogg,lock_uscita),                               \
                        (ogg,coda_segnali_uscita)                        \
                        in                                               \
                        zip(self.lock_segnali_uscita_operazioni.items(), \
                        self.coda_segnali_uscita_operazioni.items()):
                            with self.lock_segnali_uscita_operazioni[str(ogg)]:
                                self.coda_segnali_uscita_operazioni[str(ogg)].put_nowait([segnale,destinatario,mittente])

            ############## Fine ricezione messaggi dall'esterno ################
            ########## Comunicazione con le operazioni della pipeline ##########
            # Per ogni operazione
            ############## End of receiving messages from the outside #################
            ########## Communicating with Pipeline Operations ##########
            # For each operation
            for (ogg,lock_entrata),                               \
                (ogg,coda_segnali_entrata),                       \
                (ogg,lock_uscita),                                \
                (ogg,coda_segnali_uscita)                         \
                in                                                \
                zip(self.lock_segnali_entrata_operazioni.items(), \
                self.coda_segnali_entrata_operazioni.items(),     \
                self.lock_segnali_uscita_operazioni.items(),      \
                self.coda_segnali_uscita_operazioni.items()):
                #TODO: CONTROLLA DA QUI
                #TODO: CHECK IT FROM HERE
                pacchetto_segnale_entrata[:] = []
                segnale                   = ""
                mittente                  = ""
                destinatario              = ""
                timestamp                 = 0
                logging.debug(id(coda_segnali_entrata))
                # Leggi l'eventuale segnale dall'operazione
                # Read any signal from the operation
                with lock_entrata:
                    if not coda_segnali_entrata.empty():
                        pacchetto_segnale_entrata[:] = \
                         coda_segnali_entrata.get_nowait()
                logging.debug(ogg)
                logging.debug(pacchetto_segnale_entrata)
                if len(pacchetto_segnale_entrata) == 4:
                    segnale,mittente,destinatario,timestamp = \
                                                   pacchetto_segnale_entrata
                    pacchetto_segnale_entrata[:] = []
                elif len(pacchetto_segnale_entrata) == 3:
                    segnale,mittente,timestamp = pacchetto_segnale_entrata
                    pacchetto_segnale_entrata[:] = []
                elif len(pacchetto_segnale_entrata) == 0:
                    sleep(0.01)
                    continue
                else:
                    with self.lock_segnali_uscita:
                        self.coda_segnali_uscita.put_nowait( \
                                                 ["segnale mal formato",""]) # badly formed signal
                    with lock_uscita:
                        coda_segnali_uscita.put_nowait(["segnale mal formato", # badly formed signal
                                                        ""])
                    logging.info("Gestore Pipeline: Segnale mal formato") # Pipeline Manager: Badly formed signal
                    pacchetto_segnale_entrata[:] = []
                    sleep(0.01)
                    continue
                logging.debug("Gestore Pipeline " + \
                              segnale       + " " + \
                              mittente      + " " + \
                              destinatario  + " " + \
                              str(timestamp)) # Pipeline Manager
                sleep(0.001)
                # Se il destinatario è il Gestore Pipeline
                # If the recipient is the Pipeline Manager
                if str(destinatario) == type(self).__name__:
                    if segnale == "stop":
                        richiesta_stop = True
                        break
                    elif segnale == "lista_operazioni":
                        ops = ""
                        prima_operazione = 1
                        for op in self.operazioni:
                            if prima_operazione == 1:
                                ops = str(op)
                                prima_operazione = 0
                            else:
                                ops = ops + "," + str(op)
                        with self.lock_segnali_uscita_operazioni[ogg]:
                            self.coda_segnali_uscita_operazioni[ogg].put_nowait([ops,destinatario,mittente])
                # Se il destinatario è una delle altre operazioni
                # If the recipient is one of the other operations
                elif str(destinatario) in self.operazioni:
                    # Inoltra il segnale a quella specifica operazione
                    # Forwards the signal to that specific operation
                    with self.lock_segnali_uscita_operazioni[str(destinatario)]:
                        self.coda_segnali_uscita_operazioni[str(destinatario)].put_nowait([segnale,destinatario,mittente])
                # Se il destinatario è "broadcast"
                # If the recipient is "broadcast"
                elif str(destinatario) == "":
                    # Inoltra il segnale a tutte le altre operazioni
                    # Forwards the signal to all other operations
                    for operazione in self.operazioni:
                        if operazione == ogg:
                            continue
                        else:
                            with self.lock_segnali_uscita_operazioni[str(operazione)]:
                                self.coda_segnali_uscita_operazioni[str(operazione)].put_nowait([segnale,destinatario,mittente])
                        sleep(0.01)
                    if segnale == "stop":
                        richiesta_stop = True
            ############## Fine comunicazione con le operazioni ################
            ############## End of communication with operations #################
            sleep(0.01)
