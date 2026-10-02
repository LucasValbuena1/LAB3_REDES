/*
 * subscriber_tcp.c - Suscriptor (hincha) - version TCP
 * Laboratorio 3 - Analisis capa de transporte y sockets
 *
 * Se conecta al broker, se suscribe a uno o varios partidos y muestra en
 * pantalla las actualizaciones que le llegan. Revisa el numero de secuencia
 * (#n) de cada mensaje para detectar perdidas o desorden.
 *
 * Mensajes: envia "SUB <partido>\n"; recibe "<partido>|#<n> <evento>\n"
 *
 * Compilar: gcc -Wall -o subscriber_tcp subscriber_tcp.c
 * Ejecutar: ./subscriber_tcp <partido1> [partido2 ...]
 *   ej:     ./subscriber_tcp AvsB CvsD
 * Terminar: Ctrl+C (muestra el resumen de lo recibido)
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>  /* socket, connect, send, recv */

#define IP_BROKER  "127.0.0.1"
#define PUERTO     5000
#define TAM_BUF    1024
#define MAX_TEMAS  16
#define MAX_TEMA   64

/* Control de secuencia por partido */
typedef struct {
    char tema[MAX_TEMA];
    int  ultimo;         /* mayor #n recibido */
    int  recibidos;
    int  saltos;         /* veces que llego un #n mayor al esperado */
    int  desordenados;   /* veces que llego un #n menor o igual al ultimo */
} control_t;

static control_t ctrl[MAX_TEMAS];
static int num_ctrl = 0;
static volatile sig_atomic_t terminar = 0;

static void manejar_sigint(int s) { (void)s; terminar = 1; }

static control_t *buscar_control(const char *tema) {
    for (int i = 0; i < num_ctrl; i++)
        if (strcmp(ctrl[i].tema, tema) == 0) return &ctrl[i];
    if (num_ctrl == MAX_TEMAS) return NULL;
    control_t *c = &ctrl[num_ctrl++];
    memset(c, 0, sizeof(*c));
    strncpy(c->tema, tema, MAX_TEMA - 1);
    return c;
}

/* Muestra una actualizacion y verifica su numero de secuencia */
static void procesar_mensaje(char *linea) {
    char *sep = strchr(linea, '|');
    if (sep == NULL) { printf("[Suscriptor TCP] Mensaje raro: %s\n", linea); return; }
    *sep = '\0';
    const char *tema = linea;
    const char *texto = sep + 1;

    char aviso[96] = "";
    int n;
    control_t *c = buscar_control(tema);
    if (c != NULL && sscanf(texto, "#%d", &n) == 1) {
        c->recibidos++;
        if (n == c->ultimo + 1) {
            c->ultimo = n;
        } else if (n > c->ultimo + 1) {
            snprintf(aviso, sizeof(aviso), "  <-- SALTO: se esperaba #%d", c->ultimo + 1);
            c->saltos++;
            c->ultimo = n;
        } else {
            snprintf(aviso, sizeof(aviso), "  <-- FUERA DE ORDEN (ya iba en #%d)", c->ultimo);
            c->desordenados++;
        }
    }
    printf("[Suscriptor TCP] [%s] %s%s\n", tema, texto, aviso);
    fflush(stdout);
}

static void imprimir_resumen(void) {
    printf("\n===== Resumen suscriptor TCP =====\n");
    for (int i = 0; i < num_ctrl; i++)
        printf("Partido %-12s recibidos=%d  ultimo=#%d  saltos=%d  fuera_de_orden=%d\n",
               ctrl[i].tema, ctrl[i].recibidos, ctrl[i].ultimo,
               ctrl[i].saltos, ctrl[i].desordenados);
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Uso: %s <partido1> [partido2 ...]\n", argv[0]);
        return 1;
    }

    /* sigaction sin SA_RESTART: Ctrl+C interrumpe recv() (errno = EINTR)
       y asi se puede imprimir el resumen antes de salir */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = manejar_sigint;
    sigaction(SIGINT, &sa, NULL);

    /* socket(): AF_INET (IPv4) + SOCK_STREAM (TCP) */
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    struct sockaddr_in broker;
    memset(&broker, 0, sizeof(broker));
    broker.sin_family = AF_INET;
    broker.sin_port   = htons(PUERTO);
    inet_pton(AF_INET, IP_BROKER, &broker.sin_addr);

    /* connect(): handshake de tres vias con el broker */
    if (connect(fd, (struct sockaddr *)&broker, sizeof(broker)) < 0) {
        perror("connect"); close(fd); return 1;
    }
    printf("[Suscriptor TCP] Conectado al broker %s:%d\n", IP_BROKER, PUERTO);

    /* Una linea "SUB <partido>" por cada partido pedido en la linea de comandos */
    for (int i = 1; i < argc; i++) {
        char sub[MAX_TEMA + 8];
        int len = snprintf(sub, sizeof(sub), "SUB %s\n", argv[i]);
        /* send(): por ser TCP, si el segmento se pierde el SO lo retransmite */
        if (send(fd, sub, (size_t)len, 0) < 0) { perror("send"); close(fd); return 1; }
        buscar_control(argv[i]);
        printf("[Suscriptor TCP] Suscrito a '%s'\n", argv[i]);
    }
    fflush(stdout);

    char buf[TAM_BUF];
    size_t len = 0;
    while (!terminar) {
        /* recv(): bloquea hasta que llegan bytes. TCP los entrega completos y
           en orden, pero sin respetar limites de mensaje: por eso se arma cada
           mensaje buscando el '\n'. */
        ssize_t n = recv(fd, buf + len, sizeof(buf) - len - 1, 0);
        if (n < 0) {
            if (errno == EINTR) continue;   /* Ctrl+C */
            perror("recv");
            break;
        }
        if (n == 0) {   /* el broker cerro la conexion (FIN o caida) */
            printf("[Suscriptor TCP] El broker cerro la conexion\n");
            break;
        }
        len += (size_t)n;
        buf[len] = '\0';

        char *inicio = buf;
        char *nl;
        while ((nl = strchr(inicio, '\n')) != NULL) {
            *nl = '\0';
            if (*inicio != '\0') procesar_mensaje(inicio);
            inicio = nl + 1;
        }
        len = strlen(inicio);
        memmove(buf, inicio, len);
        if (len == sizeof(buf) - 1) len = 0;
    }

    close(fd);   /* close(): envia FIN al broker */
    imprimir_resumen();
    return 0;
}
