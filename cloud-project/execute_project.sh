#!/bin/bash

# Verifica che siano stati forniti due argomenti
if [ "$#" -ne 2 ]; then
    echo "Please, inserisci nell'ordine: NOME_DATASET NOME_CARTELLA_OUTPUT"
    exit 1
fi

NOME_DATASET=$1
NOME_CARTELLA_OUTPUT=$2

# Esegui il comando hadoop jar
hadoop jar ./target/cloud-project-1.0-SNAPSHOT.jar it.unipi.hadoop.LetterFrequency -D mapreduce.job.reduces=2 "$NOME_DATASET" "$NOME_CARTELLA_OUTPUT"

# Verifica se il comando è riuscito
if [ "$?" -eq 0 ]; then
    echo "Job completato con successo."
else
    echo "Errore nell'esecuzione del job."
    exit 1
fi

