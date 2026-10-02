#!/bin/bash
# run_lab.sh - Ejecuta las pruebas del Laboratorio 3 y guarda las capturas.
#
# Compila los 6 programas, y para TCP y para UDP lanza 1 broker,
# 2 suscriptores y 2 publicadores (10 mensajes cada uno) mientras tshark
# captura el trafico de la interfaz loopback (lo).
#
# Resultado:
#   tcp_pubsub.pcap, udp_pubsub.pcap   -> capturas (formato pcap)
#   logs/*.log                          -> salida de cada programa
#
# Uso: bash run_lab.sh      (pide la contrasena de sudo para capturar)

set -e
cd "$(dirname "$0")"
mkdir -p logs

echo "== Compilando =="
for f in broker_tcp broker_udp publisher_tcp publisher_udp subscriber_tcp subscriber_udp; do
    gcc -Wall -pthread -o "$f" "$f.c"
done

sudo -v   # pide la contrasena una sola vez

prueba() {
    local P=$1 PUERTO=$2
    echo "== Prueba $P (puerto $PUERTO) =="

    # Captura solo el trafico del puerto del broker, en la interfaz loopback
    sudo tshark -i lo -f "$P port $PUERTO" -F pcap -w "/tmp/${P}_pubsub.pcap" -q &
    local T=$!
    sleep 3

    ./broker_$P > logs/broker_$P.log 2>&1 & local B=$!
    sleep 1
    ./subscriber_$P AvsB CvsD > logs/suscriptor1_$P.log 2>&1 & local S1=$!
    ./subscriber_$P AvsB      > logs/suscriptor2_$P.log 2>&1 & local S2=$!
    sleep 1
    ./publisher_$P AvsB 10 500 > logs/publicador1_$P.log 2>&1 & local P1=$!
    ./publisher_$P CvsD 10 500 > logs/publicador2_$P.log 2>&1 & local P2=$!
    wait $P1 $P2
    sleep 2

    kill -INT $S1 $S2; sleep 1     # los suscriptores imprimen su resumen
    kill $B; sleep 2
    sudo kill -INT $T; wait $T 2>/dev/null || true

    sudo cp "/tmp/${P}_pubsub.pcap" "./${P}_pubsub.pcap"
    sudo chown "$USER":"$USER" "./${P}_pubsub.pcap"

    echo "--- Suscriptor 1 ($P) ---"; cat logs/suscriptor1_$P.log
    echo "--- Suscriptor 2 ($P) ---"; tail -4 logs/suscriptor2_$P.log
}

prueba tcp 5000
prueba udp 5001

echo
echo "== Listo =="
ls -l tcp_pubsub.pcap udp_pubsub.pcap
echo "Paquetes TCP: $(tshark -r tcp_pubsub.pcap 2>/dev/null | wc -l)"
echo "Paquetes UDP: $(tshark -r udp_pubsub.pcap 2>/dev/null | wc -l)"
