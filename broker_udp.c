/*
 * broker_udp.c - Broker del sistema publicacion-suscripcion (version UDP)
 * Laboratorio 3 - Analisis capa de transporte y sockets
 *
 * Rol: recibe los datagramas de los publicadores y los reenvia, sin
 * modificarlos, a los suscriptores inscritos en ese partido.
 *
 * Protocolo de aplicacion (un mensaje = un datagrama UDP):
 *   Suscriptor -> broker : "SUB <partido>"
 *   Publicador -> broker : "PUB <partido>|<mensaje>"
 *   Broker -> suscriptor : "<partido>|<mensaje>"
 *
 * UDP no tiene conexiones: no hay accept() ni un socket por cliente. El
 * broker usa UN solo socket y distingue a cada suscriptor por la direccion
 * (IP:puerto) de origen que entrega recvfrom(). Por eso tampoco necesita
 * hilos: atiende un datagrama a la vez en un ciclo.
 *
 * Solo se usa la API estandar de sockets POSIX (sin librerias externas).
 *
 * Compilar: gcc -Wall -o broker_udp broker_udp.c
 * Ejecutar: ./broker_udp
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>  /* socket, bind, recvfrom, sendto */

#define PUERTO    5001   /* puerto UDP del broker */
#define MAX_SUBS  256
#define MAX_TEMA  64
#define TAM_BUF   1024

/* Una suscripcion = direccion (IP:puerto) del suscriptor + partido */
typedef struct {
    struct sockaddr_in dir;
    char tema[MAX_TEMA];
} suscripcion_t;

static suscripcion_t subs[MAX_SUBS];
static int num_subs = 0;

static int misma_direccion(const struct sockaddr_in *a, const struct sockaddr_in *b) {
    return a->sin_addr.s_addr == b->sin_addr.s_addr && a->sin_port == b->sin_port;
}

static void agregar_suscripcion(const struct sockaddr_in *dir, const char *tema) {
    for (int i = 0; i < num_subs; i++)
        if (misma_direccion(&subs[i].dir, dir) && strcmp(subs[i].tema, tema) == 0)
            return;   /* ya estaba (p. ej. el SUB se reenvio) */
    if (num_subs == MAX_SUBS) {
        fprintf(stderr, "[Broker] Tabla de suscripciones llena\n");
        return;
    }
    subs[num_subs].dir = *dir;
    strncpy(subs[num_subs].tema, tema, MAX_TEMA - 1);
    subs[num_subs].tema[MAX_TEMA - 1] = '\0';
    num_subs++;
}

int main(void) {
    /*
     * socket(dominio, tipo, protocolo):
     *   AF_INET    -> IPv4
     *   SOCK_DGRAM -> datagramas sin conexion = UDP
     */
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    struct sockaddr_in dir;
    memset(&dir, 0, sizeof(dir));
    dir.sin_family      = AF_INET;
    dir.sin_addr.s_addr = htonl(INADDR_ANY);
    dir.sin_port        = htons(PUERTO);

    /* bind(): fija el puerto conocido donde los clientes envian sus datagramas.
       No hay listen() ni accept(): UDP no establece conexiones. */
    if (bind(fd, (struct sockaddr *)&dir, sizeof(dir)) < 0) {
        perror("bind"); close(fd); return 1;
    }
    printf("[Broker UDP] Escuchando en el puerto %d\n", PUERTO);
    fflush(stdout);

    char buf[TAM_BUF];
    for (;;) {
        struct sockaddr_in origen;
        socklen_t tam = sizeof(origen);

        /*
         * recvfrom(fd, buffer, tamano, flags, origen, tam): recibe UN
         * datagrama completo y llena 'origen' con la IP:puerto de quien lo
         * envio. Si el buffer de recepcion del SO esta lleno cuando llegan
         * datagramas, el kernel los descarta en silencio (perdida en UDP).
         */
        ssize_t n = recvfrom(fd, buf, sizeof(buf) - 1, 0,
                             (struct sockaddr *)&origen, &tam);
        if (n < 0) { perror("recvfrom"); continue; }
        buf[n] = '\0';
        if (n > 0 && buf[n - 1] == '\n') buf[n - 1] = '\0';

        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &origen.sin_addr, ip, sizeof(ip));
        int puerto = ntohs(origen.sin_port);

        if (strncmp(buf, "SUB ", 4) == 0) {
            agregar_suscripcion(&origen, buf + 4);
            printf("[Broker] %s:%d se suscribio a '%s'\n", ip, puerto, buf + 4);

        } else if (strncmp(buf, "PUB ", 4) == 0) {
            char *contenido = buf + 4;          /* "<partido>|<mensaje>" */
            char *sep = strchr(contenido, '|');
            if (sep == NULL) { printf("[Broker] PUB mal formado: %s\n", buf); continue; }

            char tema[MAX_TEMA];
            size_t lt = (size_t)(sep - contenido);
            if (lt >= MAX_TEMA) lt = MAX_TEMA - 1;
            memcpy(tema, contenido, lt);
            tema[lt] = '\0';

            int entregados = 0;
            size_t len = strlen(contenido);
            for (int i = 0; i < num_subs; i++) {
                if (strcmp(subs[i].tema, tema) != 0) continue;
                /*
                 * sendto(fd, datos, len, flags, destino, tam): envia un
                 * datagrama a la direccion indicada. Que retorne OK solo
                 * significa que salio del broker; UDP no confirma la entrega
                 * (no hay ACK ni retransmision).
                 */
                if (sendto(fd, contenido, len, 0,
                           (struct sockaddr *)&subs[i].dir, sizeof(subs[i].dir)) >= 0)
                    entregados++;
            }
            printf("[Broker] '%s' de %s:%d -> %d suscriptor(es)\n",
                   contenido, ip, puerto, entregados);

        } else {
            printf("[Broker] Comando desconocido de %s:%d: %s\n", ip, puerto, buf);
        }
        fflush(stdout);
    }

    close(fd);
    return 0;
}
