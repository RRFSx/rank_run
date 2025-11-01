/******************************************************************************
 * File: rank_run.c
 * Author: Guoqing.Ge@noaa.gov
 * Created: October 2025
 * Description:
 *     A lightweight MPI tool to run command lines or scripts on different ranks.
 *     it can be used to run serial jobs in parallel
 *
 * Wiki: https://github.com/RRFSx/rank_run/wiki
 * 
 * Example Usage:
 *     mpirun -np 10 ./rank_run.x cmdfile
 *     mpirun -np 10 ./rank_run.x 'wgrib_*.sh'
 *
 ******************************************************************************/
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#define MAX_LINE 5000
#define MAX_RANKS 5000

// Trim leading/trailing spaces and newlines
void trim(char *s) {
    char *p = s;
    while (*p && (*p==' ' || *p=='\t' || *p=='\n' || *p=='\r')) p++;
    memmove(s, p, strlen(p)+1);
    for (int i = strlen(s)-1; i>=0 && (s[i]==' '||s[i]=='\t'||s[i]=='\n'||s[i]=='\r'); i--)
        s[i]='\0';
}

int main(int argc, char *argv[]) {
    int rank, size;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc != 2) {
        if (rank==0) fprintf(stderr, "Usage: %s <cmdfile | script_pattern>\n", argv[0]);
        MPI_Finalize();
        return 1;
    }

    const char *arg = argv[1];
    char commands[MAX_RANKS][MAX_LINE] = {0};  // store lines for ranks; sets every byte to 0 by default

    // Check if argv[1] contains a '*'
    char *star = strchr(arg, '*');
    if (star) {
        // Replace * with rank number
        char scriptfile[MAX_LINE];
        snprintf(scriptfile,sizeof(scriptfile), "%.*s%d%s", (int)(star-arg), arg, rank, star+1);
        // if the script machting current rank exists, run it; otherwise no actions
        if (access(scriptfile, F_OK) == 0) {
            printf("Rank %d executing script: %s\n", rank, scriptfile);
            fflush(stdout);
            char mycmd[MAX_LINE];
            snprintf(mycmd,sizeof(mycmd),"bash %s", scriptfile);
            int ret = system(mycmd);
            if (ret != 0) {
                int exitcode = -1;
                if (WIFEXITED(ret)) exitcode = WEXITSTATUS(ret);
                fprintf(stderr, "Rank %d: command failed (exit code %d): %s\n", rank, exitcode, mycmd);
                perror("system");
                MPI_Abort(MPI_COMM_WORLD, exitcode);
            }
        } else {
            printf("[INFO] script not found: %s\n", scriptfile);
            fflush(stdout);
        }
    } else {
        // --------- Rank 0 reads the cmdfile ---------
        int num_commands = 0;
        if (rank == 0) {
            FILE *fp = fopen(arg, "r");
            if (!fp) {
                perror("fopen");
                fprintf(stderr, "file not found: '%s'\n", arg);
                MPI_Abort(MPI_COMM_WORLD, 1);
            }
            while (fgets(commands[num_commands], MAX_LINE, fp) && num_commands < MAX_RANKS) {
                trim(commands[num_commands]);
                if (commands[num_commands][0] != '\0' && commands[num_commands][0] != '#')
                    num_commands++;
            }
            fclose(fp);
        }
        // send each rank its command
        char mycmd[MAX_LINE] = "";
        if (rank == 0) {
            if (num_commands > 0) strcpy(mycmd, commands[0]);
            if (num_commands > size) {
                printf("num_commands(=%d) is larger than num_ranks(=%d), extra commands ignored!\n", num_commands, size);
            }
            for (int i=1; i<size; i++) {
                MPI_Send(commands[i], MAX_LINE, MPI_CHAR, i, 0, MPI_COMM_WORLD);
            }
        } else {
            MPI_Recv(mycmd, MAX_LINE, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }
        // run the command if not empty
        if (mycmd[0] != '\0') {
            printf("Rank %d executing command: %s\n", rank, mycmd);
            fflush(stdout);
            int ret = system(mycmd);
            if (ret != 0) {
                int exitcode = -1;
                if (WIFEXITED(ret)) exitcode = WEXITSTATUS(ret);
                fprintf(stderr, "Rank %d: command failed (exit code %d): %s\n", rank, exitcode, mycmd);
                perror("system");
                MPI_Abort(MPI_COMM_WORLD, exitcode);
            }
        }
    }

    MPI_Finalize();
    return 0;
}

