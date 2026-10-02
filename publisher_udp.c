/*
 * publisher_udp.c - Publicador (periodista deportivo) - version UDP
 * Laboratorio 3 - Analisis capa de transporte y sockets
 *
 * Envia eventos de UN partido al broker, un datagrama por evento. Cada
 * mensaje lleva un numero de secuencia (#1, #2, ...) para que el suscriptor
 * pueda detectar perdidas o desorden (UDP no lo hace).
 *
 * Datagrama enviado: "PUB <partido>|#<n> <evento>"
 *
 * Compilar: gcc -Wall -o publisher_udp publisher_udp.c
 * Ejecutar: ./publisher_udp <partido> [num_mensajes] [retardo_ms]
 *   ej:     ./publisher_udp AvsB 10 1000
 *   (con retardo_ms = 0 y muchos mensajes es mas facil ver perdidas)
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>  /* socket, sendto */

#define IP_BROKER "127.0.0.1"
#define PUERTO    5001
#define TAM_MSG   512

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

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Uso: %s <partido> [num_mensajes] [retardo_ms]\n", argv[0]);
        return 1;
    }
    const char *partido = argv[1];
    int num_mensajes = (argc > 2) ? atoi(argv[2]) : 10;
    int retardo_ms   = (argc > 3) ? atoi(argv[3]) : 1000;

    /* socket(): AF_INET (IPv4) + SOCK_DGRAM (UDP) */
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    struct sockaddr_in broker;
    memset(&broker, 0, sizeof(broker));
    broker.sin_family = AF_INET;
    broker.sin_port   = htons(PUERTO);
    inet_pton(AF_INET, IP_BROKER, &broker.sin_addr);

    /* No hay connect() ni handshake: se envia directamente. Si el broker no
       esta corriendo, el publicador no se entera (los datagramas se pierden). */
    printf("[Publicador UDP] Enviando a %s:%d - partido '%s'\n", IP_BROKER, PUERTO, partido);

    for (int i = 1; i <= num_mensajes; i++) {
        char evento[256];
        char mensaje[TAM_MSG];
        snprintf(evento, sizeof(evento), eventos[(i - 1) % NUM_EVENTOS], i * 9);
        int len = snprintf(mensaje, sizeof(mensaje), "PUB %s|#%d %s", partido, i, evento);

        /*
         * sendto(fd, datos, len, flags, destino, tam): cada llamada produce un
         * datagrama independiente (cabecera UDP de 8 bytes + datos). En el
         * primer sendto el SO asigna un puerto local efimero al socket.
         */
        if (sendto(fd, mensaje, (size_t)len, 0,
                   (struct sockaddr *)&broker, sizeof(broker)) < 0) {
            perror("sendto");
        } else {
            printf("[Publicador UDP] Enviado: %s\n", mensaje + 4);
        }
        fflush(stdout);

        if (retardo_ms > 0) usleep((useconds_t)retardo_ms * 1000);
    }

    /* close(): solo libera el socket local; no se envia nada por la red */
    close(fd);
    printf("[Publicador UDP] Fin. Mensajes enviados: %d\n", num_mensajes);
    return 0;
}
