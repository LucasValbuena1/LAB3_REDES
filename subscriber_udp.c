/*
 * subscriber_udp.c - Suscriptor (hincha) - version UDP
 * Laboratorio 3 - Analisis capa de transporte y sockets
 *
 * Envia al broker un datagrama "SUB <partido>" por cada partido de interes y
 * luego queda recibiendo las actualizaciones. Revisa el numero de secuencia
 * (#n) de cada mensaje para detectar perdidas o desorden.
 *
 * Compilar: gcc -Wall -o subscriber_udp subscriber_udp.c
 * Ejecutar: ./subscriber_udp <partido1> [partido2 ...]
 *   ej:     ./subscriber_udp AvsB CvsD
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
#include <sys/socket.h>  /* socket, sendto, recvfrom */

#define IP_BROKER  "127.0.0.1"
#define PUERTO     5001
#define TAM_BUF    1024
#define MAX_TEMAS  16
#define MAX_TEMA   64

typedef struct {
    char tema[MAX_TEMA];
    int  ultimo;
    int  recibidos;
    int  saltos;
    int  desordenados;
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

static void procesar_mensaje(char *msg) {
    char *sep = strchr(msg, '|');
    if (sep == NULL) { printf("[Suscriptor UDP] Mensaje raro: %s\n", msg); return; }
    *sep = '\0';
    const char *tema = msg;
    const char *texto = sep + 1;

    char aviso[96] = "";
    int n;
    control_t *c = buscar_control(tema);
    if (c != NULL && sscanf(texto, "#%d", &n) == 1) {
        c->recibidos++;
        if (n == c->ultimo + 1) {
            c->ultimo = n;
        } else if (n > c->ultimo + 1) {
            snprintf(aviso, sizeof(aviso), "  <-- SALTO: se esperaba #%d (perdida?)", c->ultimo + 1);
            c->saltos++;
            c->ultimo = n;
        } else {
            snprintf(aviso, sizeof(aviso), "  <-- FUERA DE ORDEN (ya iba en #%d)", c->ultimo);
            c->desordenados++;
        }
    }
    printf("[Suscriptor UDP] [%s] %s%s\n", tema, texto, aviso);
    fflush(stdout);
}

static void imprimir_resumen(void) {
    printf("\n===== Resumen suscriptor UDP =====\n");
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

    /* sigaction sin SA_RESTART: Ctrl+C interrumpe recvfrom() (EINTR) */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = manejar_sigint;
    sigaction(SIGINT, &sa, NULL);

    /* socket(): AF_INET (IPv4) + SOCK_DGRAM (UDP) */
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    struct sockaddr_in broker;
    memset(&broker, 0, sizeof(broker));
    broker.sin_family = AF_INET;
    broker.sin_port   = htons(PUERTO);
    inet_pton(AF_INET, IP_BROKER, &broker.sin_addr);

    /*
     * sendto(): un datagrama "SUB <partido>" por partido. El broker guarda la
     * IP:puerto de origen de este datagrama y la usa para responder, por eso
     * se envia y recibe por el MISMO socket. Si este datagrama se perdiera,
     * el suscriptor nunca quedaria inscrito (UDP no avisa).
     */
    for (int i = 1; i < argc; i++) {
        char sub[MAX_TEMA + 8];
        int len = snprintf(sub, sizeof(sub), "SUB %s", argv[i]);
        if (sendto(fd, sub, (size_t)len, 0, (struct sockaddr *)&broker, sizeof(broker)) < 0) {
            perror("sendto"); close(fd); return 1;
        }
        buscar_control(argv[i]);
        printf("[Suscriptor UDP] Suscripcion enviada a '%s'\n", argv[i]);
    }
    fflush(stdout);

    char buf[TAM_BUF];
    while (!terminar) {
        struct sockaddr_in origen;
        socklen_t tam = sizeof(origen);

        /* recvfrom(): recibe UN datagrama completo (los limites del mensaje
           se respetan, a diferencia de TCP), pero sin garantia de que lleguen
           todos ni en orden. */
        ssize_t n = recvfrom(fd, buf, sizeof(buf) - 1, 0, (struct sockaddr *)&origen, &tam);
        if (n < 0) {
            if (errno == EINTR) continue;   /* Ctrl+C */
            perror("recvfrom");
            break;
        }
        buf[n] = '\0';
        procesar_mensaje(buf);
    }

    close(fd);
    imprimir_resumen();
    return 0;
}
