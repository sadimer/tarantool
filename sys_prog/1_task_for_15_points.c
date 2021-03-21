#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include <ucontext.h>
#include <signal.h>
#include <sys/mman.h>
#include <aio.h>
#include <unistd.h>
#include <string.h>
#define stack_size 1024 * 1024

static ucontext_t uctx_main;
static ucontext_t *uctx_funcs;
struct arr *arrs;

static void *
allocate_stack()
{
    void *stack = malloc(stack_size);
    stack_t ss;
    ss.ss_sp = stack;
    ss.ss_size = stack_size;
    ss.ss_flags = 0;
    sigaltstack(&ss, NULL);
    return stack;
}

void 
shell_sort(long long int a[], int size)
{
    int step;
    long long int tmp;
    for (step = size / 2; step > 0; step /= 2) {
        for (int i = step; i < size; i++) {
            for (int j = i - step; j >= 0 && a[j] > a[j + step]; j -= step) {
                tmp = a[j];
                a[j] = a[j + step];
                a[j + step] = tmp;
            }
        }
    }
}

struct arr {
    long long int *a;
    int size;
};

int complited = 0;

static void 
read_and_sort(int fd, int index, int num_of_index) {
    FILE *f = fdopen(fd, "r");
    printf("coroutine %d is working...\n", index);
    swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    arrs[index].a = calloc(101, sizeof(long long int));
    int size;
    swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    for (size = 0; fscanf(f, "%lld", &arrs[index].a[size]) != -1; size++) {
        swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
        if (size % 100 == 0 && size != 0) {
            arrs[index].a = realloc(arrs[index].a, (size + 101) * sizeof(long long int));
        }
        swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    }
    arrs[index].size = size;
    swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    fclose(f);
    swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    shell_sort(arrs[index].a, size);
    swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    printf("coroutine %d stoped!\n", index);
    complited++;
    while (complited != num_of_index) {
        swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    }
}

void 
merge_sort(int n) {
    long long int writed = 0;
    FILE *wf = fopen("result.txt", "w+");
    int index[n];
    int flag[n];
    for (int i = 0; i < n; i++) {
        index[i] = 0;
        flag[i] = 0;
    }
    while(1) {
        long long int min = LLONG_MAX;
        int min_i = 0;
        for (int i = 0; i < n; i++) {
            if (arrs[i].a[index[i]] < min && flag[i] == 0) {
                min = arrs[i].a[index[i]];
                min_i = i;
            }
        }
        if (index[min_i] < arrs[min_i].size - 1) {
            index[min_i]++;
        } else {
            flag[min_i] = 1;
        }
        int res = 0;
        for (int i = 0; i < n; i++) {
            if (flag[i] == 0) {
                break;
            }
            res += 1;
        }
        fprintf(wf, "%lld ", min);
        writed++;
        if (res == n) {
            break;
        }
    }
    printf("writed %lld numbers\n", writed);
    fclose(wf);
}

int 
main(int argc, char **argv) 
{
    int fd[argc - 1];
    arrs = calloc(argc - 1, sizeof(struct arr));
    uctx_funcs = calloc(argc - 1, sizeof(ucontext_t));
    char **stacks = calloc(argc - 1, sizeof(char *));
    for (int i = 0; i < argc - 1; i++) {
        stacks[i] = allocate_stack();
        getcontext(&uctx_funcs[i]);
        uctx_funcs[i].uc_stack.ss_sp = stacks[i];
        uctx_funcs[i].uc_stack.ss_size = stack_size;
        uctx_funcs[i].uc_link = &uctx_main;
        fd[i] = open(argv[i + 1], O_RDONLY);
        makecontext(&uctx_funcs[i], read_and_sort, 3, fd[i], i, argc - 1);
    }
    swapcontext(&uctx_main, &uctx_funcs[0]);
    merge_sort(argc - 1);
    for (int i = 0; i < argc - 1; i++) {
        free(arrs[i].a);
        close(fd[i]);
        free(stacks[i]);
    }
    free(stacks);
    free(uctx_funcs);
    free(arrs);
    return 0;
}
