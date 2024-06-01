#!/bin/bash

# Verifica che siano stati forniti almeno due argomenti
if [ "$#" -lt 2 ]; then
  echo "Errore: Sono necessari almeno due argomenti."
  echo "Sintassi: script.sh NOME_CARTELLA_OUTPUT DATASET1 [DATASET2 ... DATASETN]"
  exit 1
fi

# Recupera i parametri
NOME_CARTELLA_OUTPUT=$1
# Unisci i dataset in una singola stringa separata da virgole
DATASET_LIST=$(echo "$@" | tr ' ' ',')

# Esegui il comando Hadoop
hadoop jar ./target/cloud-project-1.0-SNAPSHOT.jar it.unipi.hadoop.LetterFrequency "$DATASET_LIST" "$NOME_CARTELLA_OUTPUT"

# Verifica se il comando è riuscito
if [ "$?" -eq 0 ]; then
  echo "Job completato con successo."
else
  echo "Errore nell'esecuzione del job."
  exit 1
fi

