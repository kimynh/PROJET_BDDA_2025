#!/bin/sh
mkdir -p bin           # crée un dossier 'bin' si pas déjà présent
javac -d bin src/*.java  # compile tous les fichiers .java dans src/ et met les .class dans bin


