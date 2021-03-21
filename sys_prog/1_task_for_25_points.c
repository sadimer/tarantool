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
//принимаемые программой аргументы - имена файлов с хотябы одним числом в текстовом виде!
//результат в файле "result.txt" - отсортированная последовательсноть чисел в текстовом виде

static ucontext_t uctx_main;
static ucontext_t *uctx_funcs;


//функция создания стека для корутины
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

//для сортировки отдельных файлов используется сортировка Шелла
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

//структура хранящая массив чисел из одного файла
struct arr {
    long long int *a;
    int size;
};

//массив таких структур
struct arr *arrs;

//для отладки
int sigs = 0;
//число завершивших работу корутин
int complited = 0;
//стек пришедших сигналов о завершении чтения (обновляется каждые 100 сигналов)
int stack_of_signals[100] = {0};
//указатели для стека
int num_of_signals = 0;
int point_of_signals = 0;
//флаг прихода нового сигнала
int signals = 0;

//функция-обработчик для SIGIO
//сдвигает указатель пришедших сигналов на 1
void
checker(int sig)
{
    signal(SIGIO, checker);
    signals = 1;
    point_of_signals++;
    point_of_signals %= 100;
}

//сортировка через корутины
static void 
read_and_sort(int fd, int index, int num_of_index) {
    signal(SIGIO, checker);
    printf("coroutine %d is working...\n", index);
    swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    arrs[index].a = calloc(101, sizeof(long long int));
    swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    int size;
    swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    struct aiocb aiocb;
    swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    //буфер для посимвольного чтения, в который помещается long long
    char buf[21];
    swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    memset(&aiocb, 0, sizeof(struct aiocb));
    swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    memset(buf, 0, sizeof(buf));
    swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    aiocb.aio_fildes = fd;
    swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    aiocb.aio_sigevent.sigev_notify = SIGEV_SIGNAL;
    swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    aiocb.aio_sigevent.sigev_signo = SIGIO;
    swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    aiocb.aio_nbytes = sizeof(buf);
    swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    long long int offt = 0;
    swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    long long int old_offt = 0;
    swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    struct stat st;
    swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    fstat(fd, &st);
    swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    //цикл считывания по одному числу до конца файла
    for (size = 0; offt < st.st_size;) {
        //printf("offt: %lld index: %d offt_size: %ld\n", offt, index, st.st_size);
        swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
        aiocb.aio_buf = buf;
        swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
        aiocb.aio_offset = offt;
        swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
        aio_read(&aiocb);
        //чтение в буфер избыточного числа байт
        stack_of_signals[num_of_signals] = index;
        num_of_signals++;
        num_of_signals %= 100;
        //сдвиг счетчика ожидания сигалов на 1
        //printf("need signal on %d courutine!\n", index);
        while (1) {
            if (signals == 1) {
                //если пришел сигнал, то переключаем на ту корутину, которая этого сигнала ждала
                //для этого используем стек ожидающих сигналов
                swapcontext(&uctx_funcs[index], &uctx_funcs[stack_of_signals[point_of_signals]]);
                //printf("signaled! (%d), (%d)\n", sigs, index);
                sigs++;
                break;
            }
            //пока сигналов нет, переключаем корутины
            swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
        }
        signals = 0;
        swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
        //printf("write in %d courutine!\n", index);
        int len = 0;
        //вычисляем длину введенного числа
        swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
        for (int i = 0; i < 21; i++) {
            if (buf[i] == ' ' || buf[i] == '\0' || buf[i] == '\n') {
                break;
            }
            len++;
        }
        swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
        //если она ненулевая, то данное число записывается в массив
        if (len > 0) {
            //поскольку чтение в буфер избыточное, устанавливаем указатель чтения в корректное место (чтобы не пропустить следующие числа)
            old_offt = old_offt + len + 1;
            swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
            offt = old_offt;
            swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
            //printf("offt: %lld res: %lld strlen: %d index: %d\n", offt, strtoll(buf, NULL, 10), len, index);
            swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
            arrs[index].a[size] = strtoll(buf, NULL, 10);
            swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
            //обнуляем буфер воизбежание ошибок
            for (int i = 0; i < len; i++) {
                buf[i] = 0;
            }
            swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
            //сдвигаем переменную-итератор для массива в каждой корутине
            size++;
        }
        //printf("index: %d size: %d\n", index, size);
        swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
        //расширяем массив введенных для каждой корутины
        if (size % 100 == 0 && size != 0) {
            arrs[index].a = realloc(arrs[index].a, (size + 101) * sizeof(long long int));
        }
        swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    }
    arrs[index].size = size;
    swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    //сортировка
    shell_sort(arrs[index].a, size);
    swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    printf("coroutine %d stoped!\n", index);
    complited++;
    //при остановке корутины она автоматически переключается на следующую
    //до тех пор пока счетчик завершенных не покажет, что все завершились
    while (complited != num_of_index) {
        swapcontext(&uctx_funcs[index], &uctx_funcs[(index + 1) % num_of_index]);
    }
}

//объединяющая сортировка
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
        //из каждого массива ищем минимальное число
        //затем пишем его в выходной файл и сдвигаем в этом масиве переменную-итератор на 1
        //после чего снова из каждого из текущих элементов массивы (на которые указывают текущие итераторы)
        //выбираем минимум и пишем его в файл
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
    //массив файловых дескрипторов
    int fd[argc - 1];
    arrs = calloc(argc - 1, sizeof(struct arr));
    uctx_funcs = calloc(argc - 1, sizeof(ucontext_t));
    char **stacks = calloc(argc - 1, sizeof(char *));
    //выделяем стек и контексты для каждой корутины
    for (int i = 0; i < argc - 1; i++) {
        stacks[i] = allocate_stack();
        getcontext(&uctx_funcs[i]);
        uctx_funcs[i].uc_stack.ss_sp = stacks[i];
        uctx_funcs[i].uc_stack.ss_size = stack_size;
        uctx_funcs[i].uc_link = &uctx_main;
        fd[i] = open(argv[i + 1], O_RDONLY);
        makecontext(&uctx_funcs[i], read_and_sort, 3, fd[i], i, argc - 1);
    }
    //переключаем на 0-ю корутину
    swapcontext(&uctx_main, &uctx_funcs[0]);
    //сортируем
    merge_sort(argc - 1);
    for (int i = 0; i < argc - 1; i++) {
        free(arrs[i].a);
    }
    free(arrs);
    return 0;
}
