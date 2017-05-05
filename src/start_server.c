/**
 *  Copyright (C) 2016 George Tsatsanifos <gtsatsanifos@gmail.com>
 *
 *  #indexing is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "defs.h"
#include "stack.h"
#include "queue.h"
#include "rtree.h"
#include "spatial_standard_queries.h"
#include "qprocessor.h"
#include "getopt.h"
#include <ctype.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <time.h>

unsigned PORT;
char const* HOST;
char const* FOLDER;


unsigned DIMENSIONS;
unsigned PAGESIZE;

char const* DATASET;
char const* HEAPFILE;

size_t IO_COUNTER;

boolean INDEX_BOXES;

static char ok_response[] = "HTTP/1.0 200 OK\n"
					"Content-type: text/json\n\n"
					"{\n\t\"status\": \"%s\",\n"
					"\t\"message\": \"%s.\",\n"
					"\t\"proctime\": %u,\n"
					"\t\"data\": ";

static char bad_request_response[] = "HTTP/1.0 400 Bad Request\n"
					"Content-type: text/json\n\n"
					"{\n\t\"status\": \"ERROR\"\n,"
					"\t\"message\": \"Unable to process request.\"\n"
					"\t\"proctime\": 0.0,\n"
					"}\n";

static char not_found_response_template[] = "HTTP/1.0 404 Not Found\n"
					"Content-type: text/json\n\n"
					"{\n\t\"status\":\"ERROR\"\n,"
					"\t\"message\": \"The requested URL '%s' was not found.\"\n"
					"\t\"proctime\": 0.0,\n"
					"}\n";

static char bad_method_response_template[] = "HTTP/1.0 501 Method Not implemented\n"
					"Content-type: text/json\n\n"
					"{\n\t\"status\":\"ERROR\"\n,"
					"\t\"message\": \"The requested method '%s' is not implemented.\"\n"
					"\t\"proctime\": 0.0,\n"
					"}\n";

static
void print_notice (void) {
	puts ("\n Copyright (C) 2016 George Tsatsanifos <gtsatsanifos@gmail.com>\n");
	puts (" #indexing comes with ABSOLUTELY NO WARRANTY. This is free software, "); 
	puts (" and you are welcome to redistribute it under certain conditions.\n");
}

static
void print_usage (char const*const program) {
	printf (" ** Usage:\t %s [option] [parameter]\n", program);
	puts ("\t\t-h --host :\t The server address.");
	puts ("\t\t-p --port :\t The server port-number.");
	puts ("\t\t-f --folder :\t The folder to the path containing the heapfiles.");
}

static
void process_arguments (int argc,char *argv[]) {
	char const*const short_options = "uh:p:f:";
	const struct option long_options [] = {
		{"usage",0,NULL,'u'},
		{"host",1,NULL,'h'},
		{"port",1,NULL,'p'},
		{"folder",1,NULL,'f'},
		{NULL,0,NULL,0}
	};

	int next_option;

	do{
		next_option = getopt_long (argc,argv,short_options,long_options,NULL);

		switch (next_option) {
		case 'u':
			print_usage (argv[0]);
			exit (EXIT_SUCCESS);
		case 'h':
			HOST = optarg;
			break;
		case 'f':
			FOLDER = optarg;
			break;
		case 'p':
			PORT = atoi(optarg);
			break;
		case -1:
			break;
		case '?':
			LOG (error,"Unknown option parameter: %s\n",optarg);
		default:
			print_usage (argv[0]);
			exit (EXIT_FAILURE);
		}
	}while(next_option!=-1);
}


static
void handle (int fd, char const method[], char url[], char const folder[]) {
	LOG (info,"Server received request: %s %s\n",method,url);

	char* request = url;

	if (*request == '/') {

		size_t i = strlen(request)-1;
		while (i && request[i] == '/') {
			request [i--] = '\0';
		}
		request [i+1] = ';';
		request [i+2] = '\0';

		char* data = NULL;
		clock_t start = clock();
		if (!strcmp(method,"DELETE")) {
			LOG (info,"Processing deletion sub-request '%s'.\n",request);
		}else if (!strcmp(method,"POST")) {
			LOG (info,"Processing sub-request of type '%s'.\n",request);
		}else if (!strcmp(method,"PUT")) {
			LOG (info,"Processing insertion sub-request '%s'.\n",request);
		}else if (!strcmp(method,"GET")) {
			data = qprocessor (request,folder);
		}
		clock_t end = clock();

		boolean free_data;
		char *result_code;
		char *result_message;
		if (data != NULL) {
			free_data = true;
			result_code = "SUCCESS";
			result_message = "Successful execution";
		}else{
			free_data = false;
			data = " null\n";
			result_code = "FAILURE";
			result_message = "Syntax error";
		}

		char response[BUFSIZ];
		snprintf (response,sizeof(response),ok_response,result_code,result_message,((end-start)*1000/CLOCKS_PER_SEC));
		if (write (fd,response,strlen(response)*sizeof(char)) < strlen(response)*sizeof(char)) {
			LOG (error,"Error while sending data using file-descriptor %u.\n",fd);
		}

		if (write (fd,data,strlen(data)*sizeof(char)) < strlen(data)*sizeof(char)) {
			LOG (error,"Error while sending data using file-descriptor %u.\n",fd);
		}

		if (free_data) free (data);

		char body_end[] = "}\n";

		if (write (fd,body_end,strlen(body_end)*sizeof(char)) < strlen(body_end)*sizeof(char)) {
			LOG (error,"Error while sending data using file-descriptor %u.\n",fd);
		}
	}else{
		char response[BUFSIZ];
		snprintf (response,sizeof(response),not_found_response_template,request);
		if (write (fd,response,strlen(response)*sizeof(char)) < strlen(response)*sizeof(char)) {
			LOG (error,"Error while sending data using file-descriptor %u.\n",fd);
		}
	}
}


typedef struct {
    char* folder;
    int connection;
} handler_args;

static
void handle_connection (void const*const args) {
	char const*const folder = ((handler_args const*const)args)->folder;
	int const fd = ((handler_args const*const)args)->connection;
	free (args);

	LOG (info,"Handling new connection for file descriptor %u.\n",fd)

	char buffer[BUFSIZ];
	ssize_t bytes_read = read (fd,buffer,sizeof(buffer)-1);
	if (bytes_read) {
		char url[sizeof(buffer)];
		char method[sizeof(buffer)];
		char protocol[sizeof(buffer)];

		buffer[bytes_read] = '\0';

		sscanf (buffer,"%s %s %s",method,url,protocol);
		while (strstr(buffer,"\r\n\r\n") == NULL)
			bytes_read = read (fd,buffer,sizeof(buffer));

		if (bytes_read < 0) {
			close (fd);
			return;
		}

		if (strcmp(protocol,"HTTP/1.0") && strcmp(protocol,"HTTP/1.1")) {
			if (write (fd,bad_request_response,sizeof(bad_request_response)*sizeof(char))
												< sizeof(bad_request_response)*sizeof(char)) {
				LOG (error,"Error while sending data using file-descriptor %u.\n",fd);
			}
		}else if (strcmp(method,"GET") && strcmp(method,"PUT") && strcmp(method,"DELETE")) {
			char response[BUFSIZ];
			snprintf (response,sizeof(response),bad_method_response_template,method);
			if (write (fd,response,strlen(response)*sizeof(char)) < strlen(response)*sizeof(char)) {
				LOG (error,"Error while sending data using file-descriptor %u.\n",fd);
			}
		}else handle (fd,method,url,folder);
	}else LOG (error,"Problematic IPC...\n");
	close (fd);
}

static
void server_run (struct in_addr const local_address, uint16_t const port, char const folder[]) {
	struct sockaddr_in socket_address;
	bzero (&socket_address,sizeof(socket_address));
	socket_address.sin_family = AF_INET;
	socket_address.sin_addr = local_address;
	socket_address.sin_port = port;

	int server_socket = socket (PF_INET,SOCK_STREAM,0);
	if (server_socket < 0) LOG (error,"Cannot create a TCP socket...\n");

	if (bind (server_socket,&socket_address,sizeof(socket_address))) {
		LOG (error,"Unable to bind address. Try a different address/port pair...\n");
		return;
	}

	if (listen (server_socket,5)) {
		LOG (error,"Cannot set-up server for listening for new connections...\n");
		return;
	}

	socklen_t address_length = sizeof (socket_address);
	if (getsockname (server_socket,&socket_address,&address_length))
		LOG (warn,"Problem setting up socket-server...\n");

	LOG (info,"Server listening on '%s':%d\n",
				inet_ntoa(socket_address.sin_addr),
				ntohs(socket_address.sin_port));

	while (true) {
		struct sockaddr_in remote_address;
		address_length = sizeof (remote_address);
		int connection = accept (server_socket,&remote_address,&address_length);

		if (connection < 0) {
			/* the call to accept failed. */
			if (errno == EINTR) continue;
			else LOG (error,"Error while accepting new connection...\n");
		}else{
			address_length = sizeof (socket_address);
			if (getpeername (connection,&socket_address,&address_length)) {
				LOG (warn,"Problem establishing connection with '%s'...\n",inet_ntoa(socket_address.sin_addr));
			}else{
				LOG (info,"Server accepted new connection from '%s'.\n",inet_ntoa(socket_address.sin_addr));
			}

			handler_args *const args = (handler_args* const) malloc (sizeof (handler_args));
			args->connection = connection;
			args->folder = folder;

			pthread_t thread;
			pthread_attr_t attr;
			pthread_attr_init (&attr);
			pthread_attr_setscope (&attr,PTHREAD_SCOPE_SYSTEM);
			pthread_attr_setstacksize (&attr,THREAD_STACK_SIZE);
			pthread_attr_setdetachstate (&attr,PTHREAD_CREATE_DETACHED);
			if (pthread_create (&thread,&attr,&handle_connection,args)) {
				LOG (error,"Unable to create new thread...\n");
				exit (EXIT_FAILURE);
			}
			pthread_attr_destroy (&attr);
		}
	}
}


static 
void server_start (char const hostname[], unsigned const port_number, char const folder[]) {
	//struct hostent *local_hostname = gethostbyname (hostname);
	struct in_addr local_address;
	local_address.s_addr = hostname != NULL ? inet_addr (hostname) : INADDR_ANY;

	uint16_t port = htons(port_number);

	server_run (local_address,port,folder);
}

static char* pull_random_quote (void);

int main (int argc, char* argv[]) {
	print_notice ();
	process_arguments (argc,argv);

	if (!PORT) {
		LOG (error,"Please specify a port number...\n");
	}
	if (!FOLDER) {
		LOG (error,"Please specify the directory containing the heapfiles...\n");
	}


	if (PORT && FOLDER) {
		puts (pull_random_quote());
		server_start (HOST,PORT,FOLDER) ;
		return EXIT_SUCCESS;
	}else{
		print_usage (argv[0]);
		return EXIT_FAILURE;
	}
}

char *quotes [] = {
	"\n\t\"All along the watchtower, princes kept the view\n"
	"\t While all the women came and went, barefoot servants, too.\n\n"
	"\t Outside in the distance a wildcat did growl,\n"
	"\t Two riders were approaching, the wind began to howl.\"\n"
	"\t --------------------------------------\n"
	"\t Bob Dylan - \"All along the watchtower\"\n\n",

	"\n\t\"Did they get you to trade your heroes for ghosts?\n"
	"\t Hot ashes for trees?\n"
	"\t Hot air for a cool breeze?\n"
	"\t Cold comfort for change?\n"
	"\t Did you exchange a walk on part in the war\n" 
	"\t for a lead role in a cage?\"\n"
	"\t ---------------------------\n"
	"\t The Pink Floyd - \"Wish You Were Here\"\n\n",

	"\n\t\"There is a house in New Orleans\n"
	"\t They call the Rising Sun\n"
	"\t And it's been the ruin of many a poor boy\n"
	"\t And God I know I'm one\"\n"
	"\t ----------------------\n"
	"\t The Animals - \"House Of The Rising Sun\"\n\n",

	"\n\t\"The gate is straight, deep and wide,\n"
	"\t Break on through to the other side\"\n"
	"\t ----------------------------------\n"
	"\t The Doors - \"Break On Through (To The Other Side)\"\n\n",

	"\n\t\"Sally ,take my hand\n"
	"\t Travel south crossland\n"
	"\t Put out the fire\n"
	"\t Don't look past my shoulder\n"
	"\t The exodus is here\n"
	"\t The happy ones are near\"\n"
	"\t -----------------------\n"
	"\tThe Who - \"Baba O'Riley\"\n\n",

	"\n\t\"You need coolin\', baby, I\'m not foolin\',\n"
	"\t I\'m gonna send you back to schoolin\',\n"
	"\t Way down inside honey, you need it,\n"
	"\t I\'m gonna give you my love,\n"
	"\t I\'m gonna give you my love.\"\n"
	"\t ---------------------------\n"
	"\t Led Zeppelin - \"Whole Lotta Love\"\n\n",

	"\n\t\"It's been a long time since I rock and rolled,\n"
	"\t It's been a long time since I did the Stroll.\n"
	"\t Ooh, let me get it back, let me get it back,\n"
	"\t Let me get it back, baby, where I come from.\n"
	"\t It's been a long time, been a long time,\n"
	"\t Been a long lonely, lonely, lonely, lonely, lonely time.\"\n"
	"\t ------------------------------\n"
	"\t Led Zeppelin - \"Rock and Roll\"\n\n",

	"\n\t\"Her hair reminds me of a warm safe place\n"
	"\t Where as a child I\'d hide\n"
	"\t And pray for the thunder and the rain\n"
	"\t To quietly pass me by.\"\n"
	"\t ----------------------\n"
	"\t Guns \'n\' Roses - \"Sweet Child O' Mine\"\n\n",

	"\n\t\"I\'m a loading, a loading my war machine;\n"
	"\t I\'m contributing to the system the break down scheme.\"\n"
	"\t ---------------------------\n"
	"\t The Kyuss - \"Green Machine\"\n\n",

	"\n\t\"Fist in the air in the land of hypocrisy.\"\n"
	"\t -----------------\n"
	"\t RATM - \"Wake up\"\n\n",

	"\n\t\"I can feel it coming in the air tonight, oh Lord\n"
	"\t And I've been waiting for this moment for all my life, Oh Lord\"\n"
	"\t -----------------------------------\n"
	"\t Phil Collins - \"In The Air Tonight\"\n\n",

	"\n\t\"If only one thing that you know\n"
	"\t imposters from the show\n"
	"\t they\'ll try to trick you into\n"
	"\t normal treatment.\n"
	"\t Oh don\'t you listen to them say,\n"
	"\t shush them all away.\n"
	"\t I am the demon cleaner, madman so...\"\n"
	"\t -----------------------\n"
	"\t Kyuss - \"Demon Cleaner\""
};

static
char* pull_random_quote (void) {
	srand(time(NULL));
	return quotes[rand()%11];
}

