#!/bin/bash

# Test du SGBD avec quelques commandes de base

echo "=== Test du SGBD en mode interactif ==="
echo "Lancement du SGBD avec des commandes prédéfinies..."

# Commandes de test à envoyer au SGBD
cat << 'EOF' | java SGBD fichier_conf.json
CREATE TABLE users (id:int, name:string, age:int)
DESCRIBE users
LIST TABLES
INSERT INTO users VALUES (1, Alice, 25)
INSERT INTO users VALUES (2, Bob, 30)
SELECT * FROM users
BMSTATE
BMSETTINGS MRU
BMSTATE
DESCRIBE *
EXIT
EOF

echo "=== Test terminé ==="
