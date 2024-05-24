#!/bin/bash

# Verifica che sia stato fornito un argomento
if [ "$#" -ne 1 ]; then
    echo "Please, inserisci: $0 NOME_FILE"
    exit 1
fi

NOME_FILE=$1

# Verifica che il file esista
if [ ! -f "$NOME_FILE" ]; then
    echo "Errore: Il file '$NOME_FILE' non esiste."
    exit 1
fi

# Esegui il comando hadoop fs -put
hadoop fs -put "$NOME_FILE" "$NOME_FILE"

# Verifica se il comando è riuscito
if [ "$?" -eq 0 ]; then
    echo "File '$NOME_FILE' caricato con successo su HDFS."
else
    echo "Errore nel caricamento del file '$NOME_FILE' su HDFS."
    exit 1
fi

