/*
 * broker_tcp.c - Broker del sistema publicacion-suscripcion (version TCP)
 * Laboratorio 3 - Analisis capa de transporte y sockets
 *
 * Rol: recibe los mensajes de los publicadores (periodistas) y los reenvia,
 * sin modificarlos, a los suscriptores (hinchas) inscritos en ese partido.
 *
 * Protocolo de aplicacion (texto plano, una linea por mensaje terminada en '\n';
 * como TCP es un flujo de bytes, el '\n' sirve para delimitar los mensajes):
 *   Suscriptor -> broker : "SUB <partido>\n"
 *   Publicador -> broker : "PUB <partido>|<mensaje>\n"
 *   Broker -> suscriptor : "<partido>|<mensaje>\n"
 *
 * Concurrencia: un hilo (pthread) por cada cliente conectado. La tabla de
 * suscripciones la comparten todos los hilos, por eso se protege con un mutex.
 *
 * Solo se usa la API estandar de sockets POSIX (sin librerias externas).
 *
 * Compilar: gcc -Wall -pthread -o broker_tcp broker_tcp.c
 * Ejecutar: ./broker_tcp
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <arpa/inet.h>   /* inet_ntop, htons */
#include <netinet/in.h>  /* struct sockaddr_in, INADDR_ANY */
#include <sys/socket.h>  /* socket, bind, listen, accept, recv, send */

#define PUERTO      5000   /* puerto TCP donde escucha el broker */
#define MAX_SUBS    256    /* maximo de suscripciones (socket, partido) */
#define MAX_TEMA    64     /* longitud maxima del nombre de un partido */
#define TAM_BUF     1024   /* tamano del buffer de recepcion por cliente */

/* Una suscripcion = el socket del suscriptor + el partido que sigue */
typedef struct {
    int  fd;
    char tema[MAX_TEMA];
} suscripcion_t;

static suscripcion_t subs[MAX_SUBS];
static int num_subs = 0;
static pthread_mutex_t mutex_subs = PTHREAD_MUTEX_INITIALIZER;

/* Datos que recibe cada hilo: el socket del cliente y su direccion */
typedef struct {
    int  fd;
    char ip[INET_ADDRSTRLEN];
    int  puerto;
} cliente_t;

/*
 * send() puede enviar menos bytes de los pedidos, asi que se repite hasta
 * enviar todo. MSG_NOSIGNAL evita que el proceso muera por SIGPIPE si el
 * suscriptor ya cerro su conexion (en ese caso send devuelve -1).
 */
static int enviar_todo(int fd, const char *buf, size_t len) {
    size_t enviados = 0;
    while (enviados < len) {
        ssize_t n = send(fd, buf + enviados, len - enviados, MSG_NOSIGNAL);
        if (n <= 0) return -1;
        enviados += (size_t)n;
    }
    return 0;
}

/* Registra (fd, tema) si no existia ya */
static void agregar_suscripcion(int fd, const char *tema) {
    pthread_mutex_lock(&mutex_subs);
    for (int i = 0; i < num_subs; i++) {
        if (subs[i].fd == fd && strcmp(subs[i].tema, tema) == 0) {
            pthread_mutex_unlock(&mutex_subs);
            return;
        }
    }
    if (num_subs < MAX_SUBS) {
        subs[num_subs].fd = fd;
        strncpy(subs[num_subs].tema, tema, MAX_TEMA - 1);
        subs[num_subs].tema[MAX_TEMA - 1] = '\0';
        num_subs++;
    } else {
        fprintf(stderr, "[Broker] Tabla de suscripciones llena\n");
    }
    pthread_mutex_unlock(&mutex_subs);
}

/* Elimina todas las suscripciones de un socket (cuando el cliente se desconecta) */
static void eliminar_cliente(int fd) {
    pthread_mutex_lock(&mutex_subs);
    for (int i = 0; i < num_subs; ) {
        if (subs[i].fd == fd) {
            subs[i] = subs[num_subs - 1];   /* se reemplaza por el ultimo */
            num_subs--;
        } else {
            i++;
        }
    }
    pthread_mutex_unlock(&mutex_subs);
}

/*
 * Reenvia el mensaje a todos los suscriptores del tema. Se hace con el mutex
 * tomado para que la tabla no cambie mientras se recorre. Retorna cuantos
 * suscriptores lo recibieron.
 */
static int distribuir(const char *tema, const char *mensaje) {
    char salida[TAM_BUF + 2];
    int len = snprintf(salida, sizeof(salida), "%s\n", mensaje);
    int entregados = 0;

    pthread_mutex_lock(&mutex_subs);
    for (int i = 0; i < num_subs; i++) {
        if (strcmp(subs[i].tema, tema) == 0) {
            if (enviar_todo(subs[i].fd, salida, (size_t)len) == 0) entregados++;
        }
    }
    pthread_mutex_unlock(&mutex_subs);
    return entregados;
}

/* Interpreta una linea completa recibida de un cliente */
static void procesar_linea(cliente_t *c, char *linea) {
    if (strncmp(linea, "SUB ", 4) == 0) {
        const char *tema = linea + 4;
        agregar_suscripcion(c->fd, tema);
        printf("[Broker] %s:%d se suscribio a '%s'\n", c->ip, c->puerto, tema);

    } else if (strncmp(linea, "PUB ", 4) == 0) {
        char *contenido = linea + 4;            /* "<partido>|<mensaje>" */
        char *sep = strchr(contenido, '|');
        if (sep == NULL) {
            printf("[Broker] PUB mal formado: %s\n", linea);
            return;
        }
        char tema[MAX_TEMA];
        size_t lt = (size_t)(sep - contenido);
        if (lt >= MAX_TEMA) lt = MAX_TEMA - 1;
        memcpy(tema, contenido, lt);
        tema[lt] = '\0';

        /* Se reenvia el contenido tal cual llego (no se modifica) */
        int n = distribuir(tema, contenido);
        printf("[Broker] '%s' de %s:%d -> %d suscriptor(es)\n",
               contenido, c->ip, c->puerto, n);

    } else {
        printf("[Broker] Comando desconocido de %s:%d: %s\n", c->ip, c->puerto, linea);
    }
    fflush(stdout);
}

/* Hilo que atiende a un cliente (publicador o suscriptor) */
static void *hilo_cliente(void *arg) {
    cliente_t *c = (cliente_t *)arg;
    char buf[TAM_BUF];
    size_t len = 0;

    printf("[Broker] Nueva conexion TCP desde %s:%d\n", c->ip, c->puerto);
    fflush(stdout);

    for (;;) {
        /*
         * recv(fd, buffer, tamano, flags): lee bytes del flujo TCP.
         *   > 0 : cantidad de bytes leidos (puede traer media linea o varias)
         *   = 0 : el otro extremo cerro la conexion (FIN)
         *   < 0 : error
         */
        ssize_t n = recv(c->fd, buf + len, sizeof(buf) - len - 1, 0);
        if (n <= 0) break;
        len += (size_t)n;
        buf[len] = '\0';

        /* Se procesan todas las lineas completas que haya en el buffer */
        char *inicio = buf;
        char *nl;
        while ((nl = strchr(inicio, '\n')) != NULL) {
            *nl = '\0';
            if (nl > inicio && *(nl - 1) == '\r') *(nl - 1) = '\0';
            if (*inicio != '\0') procesar_linea(c, inicio);
            inicio = nl + 1;
        }
        /* Lo que sobra (una linea incompleta) se mueve al inicio del buffer */
        len = strlen(inicio);
        memmove(buf, inicio, len);
        if (len == sizeof(buf) - 1) len = 0;   /* linea demasiado larga: se descarta */
    }

    printf("[Broker] %s:%d se desconecto\n", c->ip, c->puerto);
    fflush(stdout);
    eliminar_cliente(c->fd);
    close(c->fd);   /* close(): libera el socket y envia FIN al cliente */
    free(c);
    return NULL;
}

int main(void) {
    /* Respaldo: si algun send escribe en un socket cerrado, no matar el proceso */
    signal(SIGPIPE, SIG_IGN);

    /*
     * socket(dominio, tipo, protocolo): crea el socket de escucha.
     *   AF_INET     -> direcciones IPv4
     *   SOCK_STREAM -> flujo confiable y orientado a conexion = TCP
     *   0           -> protocolo por defecto para ese tipo (TCP)
     */
    int fd_escucha = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_escucha < 0) { perror("socket"); return 1; }

    /*
     * setsockopt(SO_REUSEADDR): permite reiniciar el broker de inmediato sin
     * esperar a que el puerto salga del estado TIME_WAIT.
     */
    int opcion = 1;
    setsockopt(fd_escucha, SOL_SOCKET, SO_REUSEADDR, &opcion, sizeof(opcion));

    /* Direccion local: cualquier interfaz (INADDR_ANY) y el puerto PUERTO.
       htons/htonl convierten al orden de bytes de red (big-endian). */
    struct sockaddr_in dir;
    memset(&dir, 0, sizeof(dir));
    dir.sin_family      = AF_INET;
    dir.sin_addr.s_addr = htonl(INADDR_ANY);
    dir.sin_port        = htons(PUERTO);

    /* bind(): asocia el socket a la IP y puerto locales */
    if (bind(fd_escucha, (struct sockaddr *)&dir, sizeof(dir)) < 0) {
        perror("bind"); close(fd_escucha); return 1;
    }

    /* listen(fd, backlog): pasa el socket a modo pasivo. backlog = cuantas
       conexiones pendientes (handshake terminado, sin accept) puede encolar. */
    if (listen(fd_escucha, 16) < 0) {
        perror("listen"); close(fd_escucha); return 1;
    }

    printf("[Broker TCP] Escuchando en el puerto %d\n", PUERTO);
    fflush(stdout);

    for (;;) {
        struct sockaddr_in dir_cliente;
        socklen_t tam = sizeof(dir_cliente);

        /*
         * accept(): bloquea hasta que un cliente completa el handshake de
         * tres vias (SYN, SYN-ACK, ACK). Devuelve un socket NUEVO, dedicado
         * solo a ese cliente; fd_escucha sigue aceptando a los demas.
         */
        int fd_cliente = accept(fd_escucha, (struct sockaddr *)&dir_cliente, &tam);
        if (fd_cliente < 0) { perror("accept"); continue; }

        cliente_t *c = malloc(sizeof(cliente_t));
        if (c == NULL) { close(fd_cliente); continue; }
        c->fd = fd_cliente;
        inet_ntop(AF_INET, &dir_cliente.sin_addr, c->ip, sizeof(c->ip));
        c->puerto = ntohs(dir_cliente.sin_port);

        /* Un hilo por cliente; detach para que libere sus recursos al terminar */
        pthread_t hilo;
        if (pthread_create(&hilo, NULL, hilo_cliente, c) != 0) {
            perror("pthread_create");
            close(fd_cliente);
            free(c);
            continue;
        }
        pthread_detach(hilo);
    }

    close(fd_escucha);
    return 0;
}
