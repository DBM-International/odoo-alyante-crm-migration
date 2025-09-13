# Gestione Attività Progetto - DBM Module

## Panoramica

Il modulo DBM ora include la gestione delle attività/lavori dei progetti utilizzando il modello standard `project.task` di Odoo, permettendo di importare e gestire le attività associate ai progetti tramite codice commessa.

## Funzionalità Principali

### 1. Estensione Modello Task (`project.task`)

Il modulo estende il modello standard `project.task` con i seguenti campi personalizzati:

- **Codice Commessa**: Codice nel formato XXXXX-YY (es. 00001-24)
- **Azienda**: Cliente/azienda associata
- **Etichette Macro Tipo**: Etichette per categorizzare il tipo di macro attività
- **Etichette Tipo Attività**: Etichette per il tipo specifico di attività

### 2. Estensione Etichette Task (`project.tags`)

Il modulo estende il modello standard `project.tags` con una categoria per distinguere i tipi di etichette:

#### Macro Tipo (categoria `macro_type`):
- Memo
- Visita
- Attività generi
- SERVICE

#### Tipo Attività (categoria `activity_type`):
- Riunione
- Telefonata
- Email
- Documento
- Manutenzione
- Installazione

## Importazione CSV

### Formato File CSV

Il file CSV deve contenere le seguenti colonne:

| Colonna CSV | Campo Odoo | Descrizione |
|-------------|------------|-------------|
| Attività | name | Nome dell'attività (obbligatorio) |
| Azienda | company_id | Nome dell'azienda cliente |
| In carico a | user_id | Nome dell'utente responsabile |
| Data | date_deadline | Data scadenza dell'attività (formato DD/MM/YYYY o DD/MM/YYYY HH:MM) |
| Tempo | planned_hours | Durata pianificata (attualmente non gestita) |
| Fatta/da fare | stage_id | Stato dell'attività |
| Macro ti | macro_type_tag_ids | Etichette macro tipo (separate da virgola) |
| Commessa | project_code | Codice commessa nel formato XXXXX-YY |
| Descrizione attività | description | Descrizione dettagliata |
| Tipo attività | activity_type_tag_ids | Etichette tipo attività (separate da virgola) |

### Mapping Stati

I valori nella colonna "Fatta/da fare" vengono mappati agli stati standard di `project.task.stage`:
- `FATTA`, `DONE`, `COMPLETATA`, `COMPLETED` → "Completata"
- `DA FARE`, `TO DO` → "Da fare"
- `IN CORSO`, `IN PROGRESS` → "In corso"
- `ANNULLATA`, `CANCELLED`, `CANCELED` → "Annullata"

Se lo stato non viene trovato, viene utilizzato lo stato predefinito del progetto.

### Gestione Codice Commessa

- Il sistema cerca automaticamente il progetto corrispondente al codice commessa
- Se il progetto non viene trovato, l'attività viene creata comunque (come richiesto)
- Il formato del codice deve essere XXXXX-YY (es. 00001-24)

### Gestione Utenti

- Se l'utente specificato in "In carico a" non viene trovato, viene utilizzato l'utente corrente
- Se il campo è vuoto, viene utilizzato l'utente corrente

## Viste Disponibili

### 1. Vista Lista
- Mostra tutte le attività con filtri e raggruppamenti
- Filtri predefiniti: Completate, Da fare, Oggi, Questa settimana, Questo mese, Mie attività

### 2. Vista Kanban
- Visualizzazione a colonne per stato
- Colori diversi per ogni stato
- Etichette visibili per macro tipo e tipo attività

### 3. Vista Form
- Form dettagliato per la creazione/modifica
- Pulsante per toggle stato completato
- Gestione etichette con widget many2many_tags

## Menu di Navigazione

- **Attività** → **Attività Progetto**: Lista principale delle attività
- **Attività** → **Configurazione** → **Stati Attività**: Gestione stati
- **Attività** → **Configurazione** → **Etichette Attività**: Gestione etichette

## Utilizzo

1. **Importazione**: Utilizzare il wizard di importazione DBM selezionando "Attività" come tipo di importazione
2. **Creazione Manuale**: Creare nuove attività direttamente dalla vista lista o form
3. **Gestione Stati**: Utilizzare il pulsante "Completata" nella vista form o modificare lo stato direttamente
4. **Filtri**: Utilizzare i filtri predefiniti per trovare rapidamente le attività

## Note Tecniche

- Le attività vengono collegate automaticamente ai progetti tramite il codice commessa
- Le etichette vengono create automaticamente se non esistono
- Gli stati vengono creati automaticamente se non esistono
- Il sistema gestisce diversi formati di data per l'importazione
- Tutti gli errori di importazione vengono registrati nel log di Odoo
