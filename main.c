#include "common.h"
#include "ipc.h"
#include "pa2345.h"
#include <ctype.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <getopt.h>
#include <time.h>
#include <sys/types.h>
#include <sys/wait.h>

typedef int connections[MAX_PROCESS_ID + 1][MAX_PROCESS_ID + 1][2];

typedef struct queue_t
{
	int process_id;
	timestamp_t time;
	struct queue_t* next;
	
} queue_t;


// pipes[0,1] interpreted as 0 can write/read to/from 1
int N = 10;
connections pipes;
local_id local_pid;
pid_t pid, parent;
FILE *pipes_log_fd, *events_log_fd;
//int accounts[MAX_PROCESS_ID + 1];
timestamp_t lamport_time[MAX_PROCESS_ID + 2];
int mutexl = 0;

queue_t* first = NULL;

static const char* const open_pipe = "Process %5d number %2d has opened the descriptor %2d\n";
static const char* const close_pipe = "Process %5d number %2d has closed the descriptor %2d\n";
static const char* const usage_const = "Usage: %s -p proc_count val1, [val2...], val1 - value on i account\n";

void log_pipe (int fd, int open,  FILE* log)
{
	int r;
	char buf[256];
	if (open)
		sprintf(buf, open_pipe, pid, local_pid, fd);
	else sprintf(buf, close_pipe, pid, local_pid, fd);

	//printf("%s",buf);
	r = fprintf(log, "%s",buf);
	fflush(log);

}


void assert(int condition, char* message)
{
	if(!condition)
		{
			printf("%s \n errno = %d\n",message, errno);

			exit(EXIT_FAILURE);
		}

}
timestamp_t get_lamport_time()
{
	return lamport_time[local_pid];	
}

void check_recv(MessageHeader* msg_h)
{
	
	timestamp_t now = get_lamport_time();
	//printf("now %d from %d\n", now, local_pid);

	if (msg_h->s_local_time > now)
	{
		now = msg_h->s_local_time;
	//	printf("updated now is %d from %d\n", now, local_pid);
	}

	lamport_time[local_pid] = now + 1;
	//printf("new lamport is %d from %d\n", lamport_time[local_pid], local_pid);
}

int execute_critical()
{
	int M = local_pid * 5;
//	char buf[256];
	int done_counter = 0;
	//static const char * const log_loop_operation_fmt =
   // "process %1d is doing %d iteration out of %d\n";
	for(int i = 1; i <= M; i++)
	{
		char* bufd = (char*)malloc((strlen(log_loop_operation_fmt) + 1) * sizeof(char));

		sprintf(bufd, log_loop_operation_fmt, local_pid, i, M);
		if(mutexl)
			done_counter += request_cs((void*)&pipes);

		printf("Here is the critical sec of %d\n", local_pid);
		//print(bufd);

		if(mutexl)
			release_cs((void*)pipes);
	}
	return done_counter;
}

int compare(queue_t* first, queue_t* second)
{
	if(first == NULL || second == NULL)
		return -2;

	return (first->time > second->time) ? 1 : (first->time < second-> time ? -1 : (first->process_id > second->process_id ? 1 : 
		(first->process_id < second->process_id ? -1 : 0)));
}
queue_t* insert(timestamp_t time, local_id pid)
{
	queue_t *node = (queue_t*)malloc(sizeof(queue_t)), *tmp;

	node->time = time;
	node->process_id = pid;
	node->next = NULL;

	if(first == NULL)
	{
		first = node;
		return node;
	}
	
	tmp = first;


	printf("try to insert in %d\n", local_pid);
	
	if (compare(first, node)) {

		printf("compare is on first (%d)\n", local_pid);
		node->next = first;
		first = node;
		printf("On first before return (%d)\n", local_pid);
		return node;
	}
	while(tmp->next != NULL || compare(node, tmp->next))
		tmp = tmp->next;
	
	printf("while is ended (%d)\n", local_pid);
	queue_t* next = tmp->next;
	tmp->next = node;
	node->next = next;
	printf("new node is %d proc %d time\n", node->process_id, node->time);
	return node;
}

int delete(queue_t* element)
{
	queue_t *prev = first, *tmp = first;
	
	if (!compare(first, element)) {
		first = first->next;
		free(prev);
		return 0;	
	}
	
	do {
		prev = tmp;
		tmp = tmp->next;
		if (tmp == NULL)
			return -1;
	} while(compare(tmp, element) != 0);

	prev->next = tmp->next;
	free(tmp);
	return 0;
}

void print_queue()
{
	queue_t* tmp = first;
	printf("=========== queue ==============\n");
	while(tmp != NULL)
	{
		printf("proc id %d and time %d\n", tmp->process_id, tmp->time);
		tmp = tmp->next;
	}
	printf("=============end queue ============\n");
}
int request_cs(const void* self)
{
	Message msg;
	
	//queue[queue_ptr] = local_pid;
	//queue_ptr++;
	
	lamport_time[local_pid]++;

	msg.s_header.s_magic = MESSAGE_MAGIC;
	msg.s_header.s_payload_len = 0;
	msg.s_header.s_type =  CS_REQUEST;
	msg.s_header.s_local_time = get_lamport_time();

	//int request_timestamp = msg.s_header.s_local_time;
	int done_cnt = 0;

	send_multicast((void*)&pipes, &msg);
	int requests_got = N - 1;
	
	insert(msg.s_header.s_local_time, local_pid);
	print_queue();
	//int time_flag = 1;
	while(1)
	{

		int rez = receive_any((void*)&pipes, &msg);
		//printf("%d received from %d with type %d\n", local_pid, rez, msg.s_header.s_type );
		check_recv(&msg.s_header);

		if(msg.s_header.s_type == CS_REQUEST)
		{
			printf("%d received REQUEST from %d\n", local_pid, rez);
			queue_t* node = insert(msg.s_header.s_local_time,  local_pid);
			printf("inserted %d proc id %d time (%d)\n", node->process_id, node->time, local_pid);
			print_queue();

			lamport_time[local_pid]++;

			msg.s_header.s_magic = MESSAGE_MAGIC;
			msg.s_header.s_payload_len = 0;
			msg.s_header.s_type =  CS_REPLY;
			msg.s_header.s_local_time = get_lamport_time();
			
			if(rez != -1) 
				send((void*)&pipes, rez, &msg);
			printf("%d sent REPLY to %d\n", local_pid, rez);
		}
		else if(msg.s_header.s_type == CS_REPLY)
		{
			printf("%d received REPLY from %d\n", local_pid, rez);
			requests_got--;
			printf("REPLY from %d got, requests count is %d\n", rez, requests_got);
			//if(msg.s_header.s_local_time <= request_timestamp)
			//	time_flag = 0;
		}
		else if (msg.s_header.s_type == CS_RELEASE)
		{
			printf("%d received RELEASE from %d\n", local_pid, rez);
			queue_t* element = (queue_t*)malloc(sizeof(queue_t));
			element->time = msg.s_header.s_local_time;
			element->process_id = rez;
			delete(element);
		}
		else if (msg.s_header.s_type == DONE)
		{
			printf("%d received REQUEST from %d IN REQUEST\n", local_pid, rez);
			done_cnt++;
		}
		if (requests_got == 0 && first->process_id == local_pid)
			break;
	}

	return  done_cnt;
}

queue_t* queue_search(queue_t* root, int process_id)
{		
	queue_t* current = root;
	while(current->process_id != process_id)
		current = current->next;
	
	return current;
	
}
int release_cs(const void* self)
{
	Message msg;
	queue_t* element = (queue_t*)malloc(sizeof(queue_t));


	lamport_time[local_pid]++;
	
	element->time = get_lamport_time();
	element->process_id = local_pid;
	delete(element);

	printf("Release %d\n", local_pid);
	msg.s_header.s_magic = MESSAGE_MAGIC;
	msg.s_header.s_payload_len = 0;
	msg.s_header.s_type =  CS_RELEASE;
	msg.s_header.s_local_time = get_lamport_time();

	//int request_timestamp = msg.s_header.s_local_time;

	send_multicast((void*)self, &msg);

	return 0;
}

int child()
{
	int i;
	//beginning
	Message msg;
	MessageHeader msg_h;
	char buf[256];
//	first = (queue_t*)malloc(sizeof(queue_t));
	//timestamp_t time_a = 0;

	fprintf(events_log_fd, log_started_fmt, get_lamport_time(), local_pid, pid, parent,0);

	fflush(events_log_fd);
	printf("%s",msg.s_payload);

	lamport_time[local_pid]++;
	
	sprintf(msg.s_payload, log_started_fmt, get_lamport_time(), local_pid, pid, parent,0);
	//printf("%ds balance %d\n", local_pid, history->s_history[0].s_balance);

	msg_h.s_magic = MESSAGE_MAGIC;
	msg_h.s_payload_len = strlen(msg.s_payload) + 1;//why + 1??..
	msg_h.s_type = STARTED;
	msg.s_header = msg_h;
	msg.s_header.s_local_time = get_lamport_time();

	//printf("lamport BEFORE %d proc STARTED: %d\n", local_pid, msg.s_header.s_local_time);
	assert(send_multicast((void*)&pipes, &msg) != 0 ? 0 : 1, "Error on sending multicast STARTED messages\n");
	
	//printf("lamport AFTER %d proc STARTED: %d\n", local_pid, get_lamport_time());
	for(i = 1; i < N + 1; i++)
		if (i != local_pid)
			{
				int rez = -1;
				while(rez == -1)
					rez = receive((void*)&pipes, i, &msg);

				check_recv(&msg.s_header); ///????? after every receive or after all receives?
			}

	
	sprintf(buf,log_received_all_started_fmt, get_lamport_time(), local_pid );
	printf("%s",buf);

	fprintf(events_log_fd,"%s", buf);
	fflush(events_log_fd);

	//received all STARTED messages
	//work begins here
	int done_counter = execute_critical();
	
	//work ends here
	lamport_time[local_pid]++;

	sprintf(msg.s_payload, log_done_fmt, get_lamport_time(), local_pid, 0);

	msg_h.s_magic = MESSAGE_MAGIC;
	msg_h.s_payload_len = 0;
	msg_h.s_type = DONE;
	msg_h.s_local_time = get_lamport_time();

	msg.s_header = msg_h;

	//log done fmt
	printf("%s",msg.s_payload);
	fprintf(events_log_fd, "%s",msg.s_payload);
	fflush(events_log_fd);

	assert(send_multicast((void*)&pipes, &msg) != 0 ? 0 : 1, "Error while sending multicast DONE messages\n");

	while(done_counter < N - 1)
	{
	    receive_any((void*)pipes, &msg);

		check_recv(&msg.s_header);

		if(msg.s_header.s_type == DONE)
		{
			done_counter++;
			//printf("%d received DONE from %d, DONE COUNTER is %d\n", local_pid, rez, done_counter);
			
		}
		else if (msg.s_header.s_type == CS_REQUEST)
		{
			lamport_time[local_pid]++;

			msg.s_header.s_magic = MESSAGE_MAGIC;
			msg.s_header.s_payload_len = 0;
			msg.s_header.s_type =  CS_REPLY;
			msg.s_header.s_local_time = get_lamport_time();

			send((void*)pipes, local_pid, &msg);
		}
	}
	//for(i = 1; i < N + 1; i++)
	//	if(i != local_pid)
		//	{
			//	int rez = -1;
			//	while(rez == -1)
				//	rez = receive((void*)pipes, i,  &msg);
			//	check_recv(&msg.s_header);
			//}
//printf("%d've received %s\n", local_pid,msg.s_payload);

	
	sprintf(buf,log_received_all_done_fmt, get_lamport_time(), local_pid);
	printf("%s",buf);

	fprintf(events_log_fd,"%s", buf);
	fflush(events_log_fd);

	
	//after has got all DONE exits
	exit(0);
}


int send(void * self, local_id dst, const Message * msg) {
	
	int fd, rez = 0;
	//printf("%d tries to send to %d\n", local_pid, dst);

	fd = (*(connections*)self)[local_pid][dst][1];
	//printf("Process %d sends to %d\n",local_pid, dst);
	
		rez = write(fd, (void*)msg, (sizeof(msg->s_header)) + sizeof(char)*(msg->s_header.s_payload_len));
  printf("%d sent to %d, result is %d\n", local_pid, dst, rez);
	return rez > 0 ? 0 : rez;
}

int send_multicast(void * self, const Message * msg) {
	int i, err = 0;

	for (i = 0; i < N + 1; i++) {
		if (i != local_pid)
			err |= send(self, i, msg);
	}

	return err;
 }

int receive(void * self, local_id from, Message * msg) {

	int rez = -1;
	int fd = (*(connections*)self)[local_pid][from][0];
	
	rez =  read(fd, &msg->s_header, sizeof(MessageHeader));
	
	if(rez != -1 && msg->s_header.s_payload_len > 0)
	{
			rez = read(fd, msg->s_payload, msg->s_header.s_payload_len);
	}

	return rez > 0 ? 0 : -1;
}

int receive_any(void * self, Message * msg){

int rez = -1, i = 0;

do{
		for (i = 0; i <= N; i++){
			if (i != local_pid){
				rez = receive(self, i, msg);
//rez = read((*(connections*)self)[local_pid][i][0], (void*)msg, sizeof(*msg));
				if(rez >= 0)
					break;
				
			}
		}
	} while (rez == -1);
	return i;
}

void close_descriptors()
{
	int c,k;
	for(c = 0; c < N + 1; c++)
		for(k = 0; k < N + 1; k++)
			{
				if(c != k && c != local_pid)
				{
					close(pipes[c][k][0]);
					log_pipe(pipes[c][k][0], 0, pipes_log_fd);
					close(pipes[c][k][1]);
					log_pipe(pipes[c][k][1], 0, pipes_log_fd);
				} 
			}
}


void usage(char* program)
{
	printf(usage_const,program);
}

int parse_num(char* str, int* num)
{
	int i;
//	char buf[256];

	for(i = 0; i < strlen(str); i++)
	{
		if(!(str[i] >= '0' && str[i] <= '9'))
		return -1;
	}
	*num = atoi(str);
	return 0;
}
int main(int argc, char ** argv)
{
	//declarations
	int c = 0, k = 0, i = 0; //, flags;

	int temp_pipes[2], temp_pipes2[2];
	char buf[256];

	pid_t pids[N + 1];
	Message msg;

	
	

	pid = getpid();
	parent = pid;
	local_pid = PARENT_ID;

	//open log descriptors
	pipes_log_fd = fopen (pipes_log, "a");

	assert(pipes_log_fd == NULL ? 0 : 1, "Cannot open pipes_log\n");

	events_log_fd = fopen (events_log, "a");

	assert(events_log_fd == NULL? 0 : 1, "Cannot open events_log\n");

	char mes[64];
	sprintf(mes, usage_const, argv[0]);
 	static struct option long_options[] = { {"mutexl", no_argument,  &mutexl, 1} };
	int option_index = 0;

	while((c = getopt_long(argc, argv,"p:",long_options,&option_index)) != -1)
		switch(c)
		{
			case 'p': 
				N = atoi(optarg);
				assert(N < 1 || N > MAX_PROCESS_ID ? 0 : 1, "The number of processes cannot be 0 or more than maximum count or \n -p argument must be a number\n");
			break;
			case '?':
				assert(optopt == 'p' ? 0 : 1, "Option -p requires an argument\n");
			break;
		}
	printf("mutexl is %d\n", mutexl);
			
	//init lamport time
	for(i = 0; i < MAX_PROCESS_ID + 2; i++)
		lamport_time[i] = 0;
	
	//[c][k][0] r // c reads from k //close from k
	//[k][c][1] w // k writes to c  //close from c
	//[k][c][0] r // k reads from c //close from c
	//[c][k][1] w // c writes to k  //close from k
	for(c = 0; c < N + 1; c++)
		for(k = c + 1; k < N + 1; k++)
		{
			//if(c != k)
			//{
				sprintf(buf, "Error while opening pipe on i = %d j = %d",c,k);
				assert(pipe(temp_pipes) == -1 ? 0 : 1,buf);

				//handle read desc 1
				log_pipe(temp_pipes[0], 1, pipes_log_fd);

				int flags = fcntl(temp_pipes[0], F_GETFL, 0);

				fcntl(temp_pipes[0], F_SETFL, flags | O_NONBLOCK);
		
				//handle write desc 1
				log_pipe(temp_pipes[1], 1, pipes_log_fd);
				
				flags = fcntl(temp_pipes[1], F_GETFL, 0);
				
				fcntl(temp_pipes[1], F_SETFL, flags | O_NONBLOCK);
				
				//open 2nd channel
				assert(pipe(temp_pipes2) == -1 ? 0 : 1,buf);

				log_pipe(temp_pipes2[0], 1, pipes_log_fd);

				//handle read desc 2
				flags = fcntl(temp_pipes2[0], F_GETFL, 0);

				fcntl(temp_pipes2[0], F_SETFL, flags | O_NONBLOCK);

				log_pipe(temp_pipes2[1], 1, pipes_log_fd);
				
				//handle write desc 2
				flags = fcntl(temp_pipes2[1], F_GETFL, 0);
				
				fcntl(temp_pipes2[1], F_SETFL, flags | O_NONBLOCK);

				pipes[c][k][0] = temp_pipes[0];
				pipes[k][c][1] = temp_pipes[1];
				pipes[k][c][0] = temp_pipes2[0];
				pipes[c][k][1] = temp_pipes2[1];


		}

	for(c = 1; c < N + 1; c++)
	{

		pid = fork();

		if(pid == 0)
		{

			local_pid = c;
			pid = getpid();
			close_descriptors();
				
			child();
		}
		else pids[c] = pid;
		
	}

	close_descriptors();
	k = 1;

	while(k <= N)
	{
		while(receive((void*)pipes, k, &msg) == -1);
		check_recv(&msg.s_header);
		//printf("Time of %d process: %d\n", k, msg.s_header.s_local_time);
		if(msg.s_header.s_type == STARTED)
		k++;
	}
	//all started messages received
	sprintf(buf,log_received_all_started_fmt, get_lamport_time(), local_pid );
	printf("%s",buf);

	fprintf(events_log_fd, "%s", buf);
	fflush(events_log_fd);

	//bank_robbery((void*)pipes, N);

	//Message msg;
	//MessageHeader msg_h;
	
//	lamport_time[local_pid]++;
//	msg_h.s_magic = MESSAGE_MAGIC;
//	msg_h.s_payload_len = 0;
//	msg_h.s_type = STOP;
//	msg_h.s_local_time = get_lamport_time();

//	msg.s_header = msg_h;

//	send_multicast((void*)&pipes, &msg);
	k = 1;
	while(k <= N)
	{
		while(receive((void*)pipes, k, &msg) == -1);
		check_recv(&msg.s_header);
		if(msg.s_header.s_type == DONE)
		k++;
	}
	//all DONE messages have been received

	sprintf(buf,log_received_all_done_fmt, get_lamport_time(), local_pid);
	printf("%s",buf);

	fprintf(events_log_fd,"%s", buf);
	fflush(events_log_fd);

	
	//printf("After for lol\n");
	for(int i = 0; i < N; ++i)
		wait(&pids[i]);


	fclose(events_log_fd);
	fclose(pipes_log_fd);

	return 0;
}
