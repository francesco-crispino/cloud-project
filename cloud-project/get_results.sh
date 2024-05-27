#!/bin/bash

# Verifica che sia stato fornito un argomento
if [ "$#" -ne 1 ]; then
    echo "Please, inserire: NOME_CARTELLA_OUTPUT"
    exit 1
fi

NOME_CARTELLA_OUTPUT=$1

# Esegui il comando hadoop fs -cat
hadoop fs -cat "${NOME_CARTELLA_OUTPUT}/letters_frequency/part*" | cat

# Verifica se il comando è riuscito
if [ "$?" -eq 0 ]; then
    echo "Contenuto della cartella '${NOME_CARTELLA_OUTPUT}' mostrato con successo."
else
    echo "Errore nel mostrare il contenuto della cartella '${NOME_CARTELLA_OUTPUT}'."
    exit 1
fi

