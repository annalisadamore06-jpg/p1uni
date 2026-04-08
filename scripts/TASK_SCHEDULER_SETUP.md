# Come configurare Windows Task Scheduler per P1UNI

## Avvio Automatico al Boot

### Metodo 1: Task Scheduler (Consigliato)

1. Apri **Task Scheduler** (Utilita di pianificazione)
   - Win+R -> `taskschd.msc` -> Invio

2. Click **"Crea attivita"** (non "Crea attivita di base")

3. Tab **Generale**:
   - Nome: `P1UNI Trading System`
   - Descrizione: `Avvia il sistema di autotrading ES futures`
   - Seleziona: `Esegui indipendentemente dalla connessione dell'utente`
   - Seleziona: `Esegui con i privilegi piu elevati`

4. Tab **Trigger**:
   - Nuovo -> `All'avvio`
   - Ritarda attivita di: `1 minuto` (per dare tempo alla rete)

5. Tab **Azione**:
   - Nuova -> `Avvia un programma`
   - **Programma**: `C:\Users\annal\Desktop\P1UNI\scripts\start_p1uni.bat`
   - **Cartella di lavoro**: `C:\Users\annal\Desktop\P1UNI`

6. Tab **Condizioni**:
   - DESELEZIONA "Avvia solo se il computer e' collegato alla rete elettrica"
   - DESELEZIONA "Arresta se il computer passa ad alimentazione a batteria"

7. Tab **Impostazioni**:
   - Seleziona: `Se l'attivita non riesce, riavvia ogni: 1 minuto`
   - `Tenta il riavvio fino a: 3 volte`
   - Seleziona: `Esegui l'attivita il prima possibile dopo un avvio pianificato mancato`
   - NON selezionare: `Arresta l'attivita se viene eseguita per piu di...`

8. Click **OK** e inserisci la password di Windows se richiesta.

### Metodo 2: Cartella Startup (Piu semplice)

1. Win+R -> `shell:startup` -> Invio
2. Crea un collegamento a `C:\Users\annal\Desktop\P1UNI\scripts\start_p1uni.bat`
3. Il .bat partira ad ogni login di Windows

**Nota**: questo metodo richiede che l'utente faccia login. Task Scheduler funziona anche senza login.

## Come funziona il Watchdog (.bat)

Lo script `start_p1uni.bat` ha un loop integrato:

```
Avvia main.py
  |
  v
main.py esce
  |
  +--> Exit code 0? -> Shutdown volontario, esci
  |
  +--> Exit code != 0? -> CRASH! Attendi 30 sec, riavvia
```

## Comandi Utili

```powershell
# Verifica che il task sia schedulato
schtasks /query /tn "P1UNI Trading System"

# Avvia manualmente il task
schtasks /run /tn "P1UNI Trading System"

# Ferma il task
schtasks /end /tn "P1UNI Trading System"

# Elimina il task
schtasks /delete /tn "P1UNI Trading System"
```

## Verifica

Dopo aver configurato:
1. Riavvia il PC
2. Dopo 1 minuto, controlla:
   - Il file `C:\Users\annal\Desktop\P1UNI\logs\p1uni.log` deve crescere
   - Task Manager deve mostrare un processo `python.exe`
3. Se non parte, controlla il Task Scheduler per errori
