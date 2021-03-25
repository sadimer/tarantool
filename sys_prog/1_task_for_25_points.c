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
#include <time.h>
#define stack_size 1024 * 1024
//принимаемые программой аргументы - имена файлов с хотябы одним числом в текстовом виде!
//результат в файле "result.txt" - отсортированная последовательсноть чисел в текстовом виде

static ucontext_t uctx_main;
static ucontext_t *uctx_funcs;

enum Constants {
    LAST_SIGNAL_TIMEOUT = 2,
    CALLOC_SIZE = 101,
    BUF_SIZE = 21
};

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

//функция просеивания для пирамидальной сортировки
void 
sift(long long int *a, int root, int bottom)
{
    int max_child;
    int done = 0;
    while ((root * 2 <= bottom) && (done != 1)) {
        if (root * 2 == bottom) {
            max_child = root * 2;
        } else if (a[root * 2] > a[root * 2 + 1]) {
            max_child = root * 2;
        } else {
            max_child = root * 2 + 1;
        }
        if (a[root] < a[max_child]) {
            int tmp = a[root];
            a[root] = a[max_child];
            a[max_child] = tmp;
            root = max_child;
        } else {
            done = 1;
        }
    }
}

//для сортировки отдельных файлов используется пирамидальная сортировка
void 
heap_sort(long long int *a, int size) 
{
    for (int i = (size / 2); i >= 0; i--) {
        sift(a, i, size - 1);
    }
    for (int i = size - 1; i >= 1; i--) {
        int tmp = a[0];
        a[0] = a[i];
        a[i] = tmp;
        sift(a, 0, i - 1);
    }
}

//структура хранящая массив чисел из одного файла
struct arr {
    long long int *a;
    int size;
};

//массив таких структур
struct arr *arrs;

//число завершивших работу корутин
int complited = 0;
//стек пришедших сигналов о завершении чтения (обновляется каждые 100 сигналов)
int *stack_of_signals;
//указатели для стека
int num_of_signals = 0;
int point_of_signals = 0;
//флаг прихода нового сигнала
int signals = 0;

void yield(int index, int new_index) {
    swapcontext(&uctx_funcs[index], &uctx_funcs[new_index]);
}

//функция-обработчик для SIGRTMIN
//сдвигает указатель пришедших сигналов на 1
void
checker(int sig)
{
    signal(SIGRTMIN, checker);
    signals = 1;
}

//сортировка через корутины
static void 
read_and_sort(int fd, int index, int num_of_index) {
    signal(SIGRTMIN, checker);
    yield(index, (index + 1) % num_of_index);
    printf("coroutine %d is working...\n", index);
    yield(index, (index + 1) % num_of_index);
    arrs[index].a = calloc(CALLOC_SIZE, sizeof(long long int));
    yield(index, (index + 1) % num_of_index);
    int size;
    yield(index, (index + 1) % num_of_index);
    struct aiocb aiocb;
    yield(index, (index + 1) % num_of_index);
    //буфер для посимвольного чтения, в который помещается long long
    char buf[BUF_SIZE];
    yield(index, (index + 1) % num_of_index);
    memset(&aiocb, 0, sizeof(struct aiocb));
    yield(index, (index + 1) % num_of_index);
    memset(buf, 0, sizeof(buf));
    yield(index, (index + 1) % num_of_index);
    aiocb.aio_fildes = fd;
    yield(index, (index + 1) % num_of_index);
    aiocb.aio_sigevent.sigev_notify = SIGEV_SIGNAL;
    yield(index, (index + 1) % num_of_index);
    aiocb.aio_sigevent.sigev_signo = SIGRTMIN;
    yield(index, (index + 1) % num_of_index);
    aiocb.aio_nbytes = sizeof(buf);
    yield(index, (index + 1) % num_of_index);
    long long int offt = 0;
    yield(index, (index + 1) % num_of_index);
    long long int old_offt = 0;
    yield(index, (index + 1) % num_of_index);
    struct stat st;
    yield(index, (index + 1) % num_of_index);
    fstat(fd, &st);
    yield(index, (index + 1) % num_of_index);
    //цикл считывания по одному числу до конца файла
    for (size = 0; offt < st.st_size;) {
        yield(index, (index + 1) % num_of_index);
        aiocb.aio_buf = buf;
        yield(index, (index + 1) % num_of_index);
        aiocb.aio_offset = offt;
        yield(index, (index + 1) % num_of_index);
        aio_read(&aiocb);
        time_t lt = time(NULL);
        //чтение в буфер избыточного числа байт
        stack_of_signals[num_of_signals] = index;
        num_of_signals++;
        num_of_signals %= num_of_index;
        yield(index, (index + 1) % num_of_index);
        //сдвиг счетчика ожидания сигалов на 1
        while (1) {
            //если пришел сигнал или истекло время ожидание сигнала
            if (signals == 1 || ((time(NULL) - lt) > LAST_SIGNAL_TIMEOUT)) {
                int save_index = stack_of_signals[point_of_signals];
                stack_of_signals[point_of_signals] = -1;
                point_of_signals++;
                point_of_signals %= num_of_index;
                //если пришел сигнал, то переключаем на ту корутину, которая этого сигнала ждала
                //для этого используем стек ожидающих сигналов
                yield(index, save_index);
                signals = 0;
                break;
            }
            //пока сигналов нет, переключаем корутины
            yield(index, (index + 1) % num_of_index);
        }
        yield(index, (index + 1) % num_of_index);
        int len = 0;
        //вычисляем длину введенного числа
        yield(index, (index + 1) % num_of_index);
        for (int i = 0; i < BUF_SIZE; i++) {
            if (buf[i] == ' ' || buf[i] == '\0' || buf[i] == '\n' || buf[i] == EOF) {
                break;
            }
            len++;
        }
        yield(index, (index + 1) % num_of_index);
        //если она ненулевая, то данное число записывается в массив
        if (len > 0) {
            //поскольку чтение в буфер избыточное, устанавливаем указатель чтения в корректное место (чтобы не пропустить следующие числа)
            old_offt = old_offt + len + 1;
            yield(index, (index + 1) % num_of_index);
            offt = old_offt;
            yield(index, (index + 1) % num_of_index);
            arrs[index].a[size] = strtoll(buf, NULL, 10);
            yield(index, (index + 1) % num_of_index);
            //обнуляем буфер воизбежание ошибок
            for (int i = 0; i < BUF_SIZE; i++) {
                buf[i] = 0;
            }
            yield(index, (index + 1) % num_of_index);
            //сдвигаем переменную-итератор для массива в каждой корутине
            size++;
        }
        yield(index, (index + 1) % num_of_index);
        //расширяем массив введенных для каждой корутины
        if (size % (CALLOC_SIZE - 1) == 0 && size != 0) {
            arrs[index].a = realloc(arrs[index].a, (size + CALLOC_SIZE) * sizeof(long long int));
        }
        yield(index, (index + 1) % num_of_index);
    }
    arrs[index].size = size;
    yield(index, (index + 1) % num_of_index);
    //сортировка
    heap_sort(arrs[index].a, size);
    yield(index, (index + 1) % num_of_index);
    printf("coroutine %d stoped!\n", index);
    complited++;
    //при остановке корутины она автоматически переключается на следующую
    //до тех пор пока счетчик завершенных не покажет, что все завершились
    while (complited != num_of_index) {
        yield(index, (index + 1) % num_of_index);
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
    stack_of_signals = calloc(argc - 1, sizeof(stack_of_signals));
    for (int i = 0; i < argc - 1; i++) {
        stack_of_signals[i] = -1;
    }
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
        close(fd[i]);
        free(stacks[i]);
    }
    free(stacks);
    free(uctx_funcs);
    free(arrs);
    return 0;
}
