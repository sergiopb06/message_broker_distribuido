#ifndef REPLICATION_H
#define REPLICATION_H

typedef enum { 
    ADMIN_PAUSE, 
    ADMIN_RESUME, 
    ADMIN_SHUTDOWN 
} admin_cmd_t;

int cluster_admin(int broker_id, admin_cmd_t command);

#endif //REPLICATION_H