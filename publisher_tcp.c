/*
 * publisher_tcp.c - Publicador (periodista deportivo) - version TCP
 * Laboratorio 3 - Analisis capa de transporte y sockets
 *
 * Se conecta al broker y envia eventos de UN partido. Cada mensaje lleva un
 * numero de secuencia (#1, #2, ...) para que el suscriptor pueda verificar si
 * llegaron todos y en orden.
 *
 * Mensaje enviado: "PUB <partido>|#<n> <evento>\n"
 *
 * Compilar: gcc -Wall -o publisher_tcp publisher_tcp.c
 * Ejecutar: ./publisher_tcp <partido> [num_mensajes] [retardo_ms]
 *   ej:     ./publisher_tcp AvsB 10 1000
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>   /* inet_pton, htons */
#include <netinet/in.h>
#include <sys/socket.h>  /* socket, connect, send */

#define IP_BROKER     "127.0.0.1"
#define PUERTO        5000
#define TAM_MSG       512

/* Eventos de ejemplo del partido (se recorren en ciclo) */
static const char *eventos[] = {
    "Inicio del partido",
    "Gol de Equipo A al minuto %d",
    "Tarjeta amarilla al numero 10 de Equipo B al minuto %d",
    "Cambio: jugador 10 entra por jugador 20 de Equipo A al minuto %d",
    "Gol de Equipo B al minuto %d",
    "Tiro de esquina para Equipo A al minuto %d",
    "Tarjeta roja al numero 5 de Equipo A al minuto %d",
    "Penal a favor de Equipo B al minuto %d",
    "Gol de Equipo B al minuto %d",
    "Final del partido",
};
#define NUM_EVENTOS (int)(sizeof(eventos) / sizeof(eventos[0]))

/* Repite send() hasta que se hayan enviado todos los bytes */
static int enviar_todo(int fd, const char *buf, size_t len) {
    size_t enviados = 0;
    while (enviados < len) {
        ssize_t n = send(fd, buf + enviados, len - enviados, 0);
        if (n <= 0) return -1;
        enviados += (size_t)n;
    }
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Uso: %s <partido> [num_mensajes] [retardo_ms]\n", argv[0]);
        return 1;
    }
    const char *partido = argv[1];
    int num_mensajes = (argc > 2) ? atoi(argv[2]) : 10;
    int retardo_ms   = (argc > 3) ? atoi(argv[3]) : 1000;

    /* socket(): AF_INET (IPv4) + SOCK_STREAM (TCP) */
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    /* Direccion del broker; inet_pton convierte "127.0.0.1" a binario */
    struct sockaddr_in broker;
    memset(&broker, 0, sizeof(broker));
    broker.sin_family = AF_INET;
    broker.sin_port   = htons(PUERTO);
    if (inet_pton(AF_INET, IP_BROKER, &broker.sin_addr) != 1) {
        fprintf(stderr, "IP invalida\n"); close(fd); return 1;
    }

    /*
     * connect(): inicia el handshake de tres vias con el broker. El sistema
     * operativo asigna automaticamente un puerto local efimero. Si el broker
     * no esta corriendo, falla con "Connection refused" (llega un RST).
     */
    if (connect(fd, (struct sockaddr *)&broker, sizeof(broker)) < 0) {
        perror("connect"); close(fd); return 1;
    }
    printf("[Publicador TCP] Conectado al broker %s:%d - partido '%s'\n",
           IP_BROKER, PUERTO, partido);

    for (int i = 1; i <= num_mensajes; i++) {
        char evento[256];
        char mensaje[TAM_MSG];

        /* El minuto avanza con cada evento (solo para que se vea realista) */
        snprintf(evento, sizeof(evento), eventos[(i - 1) % NUM_EVENTOS], i * 9);
        int len = snprintf(mensaje, sizeof(mensaje), "PUB %s|#%d %s\n",
                           partido, i, evento);

        /* send(): escribe en el flujo TCP; TCP se encarga de segmentar,
           numerar (secuencia/ACK) y retransmitir si algo se pierde */
        if (enviar_todo(fd, mensaje, (size_t)len) < 0) {
            perror("send");
            break;
        }
        printf("[Publicador TCP] Enviado: %s", mensaje + 4);
        fflush(stdout);

        if (retardo_ms > 0) usleep((useconds_t)retardo_ms * 1000);
    }

    /* close(): envia FIN y cierra la conexion de forma ordenada */
    close(fd);
    printf("[Publicador TCP] Fin. Mensajes enviados: %d\n", num_mensajes);
    return 0;
}
