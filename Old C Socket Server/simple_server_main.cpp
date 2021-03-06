#include "ServerSocket.h"
#include "SocketException.h"
#include <cstring>
#include <pthread.h>
#include <vector>
using namespace std;

vector<string> splitStringIntoTokens(char *data) {
	vector<string> tokens;
	char *pch = strtok (data, ";");
	while (pch != NULL)
	{
		tokens.push_back(string(pch));
		pch = strtok (NULL, ";");
	}
	return tokens;
}

bool processAndGetPermission(string data) {
	char *nonconstdata = new char[data.length() + 1];
	strcpy(nonconstdata, data.c_str());
	vector<string> tokens = splitStringIntoTokens(nonconstdata);
	for(auto it = tokens.begin(); it!=tokens.end(); it++)
		cout << *it << endl;

	return false;
}

void * serveRequest(void * inputsock) {
	ServerSocket* new_sock = NULL;
	try {
		new_sock = (ServerSocket *) inputsock;

		std::string data;
		while (true) {
			if (!new_sock->recv(data)) {
				throw SocketException("Could not write to socket.");
			}

			bool permission = processAndGetPermission(data);
			if (!new_sock->send(data)) {
				throw SocketException("Could not write to socket.");
			}
		}
	} catch (SocketException&) {
		pthread_exit(NULL);
	}

	if (new_sock != NULL) {
		new_sock->closeClient();
		delete new_sock;
	}
}

int main(int argc, int argv[]) {
	std::cout << "Starting Server....\n";

	try {
		// Create the socket
		ServerSocket server(30000);
		std::cout << "Server Started\n";
		pthread_t threads[MAXCONNECTIONS];
		int no_threads = -1;

		while (true) {

			ServerSocket* new_sock = new ServerSocket();
			server.accept(new_sock);
			no_threads++;

			if (no_threads >= MAXCONNECTIONS) {
				std::cout << " Max connections reached - " << MAXCONNECTIONS
						<< std::endl;
				new_sock->send("-1");
				new_sock->closeClient();
				delete new_sock;
				continue;
			} else {
				new_sock->send("1");
			}

			int rc = pthread_create(&threads[no_threads], NULL, serveRequest,
					(void *) new_sock);
			if (rc) {
				throw SocketException("Error:unable to create thread ");
			}

			std::cout << " Created thread " << new_sock << std::endl;
		}
	} catch (SocketException& e) {
		std::cout << "Exception was caught:" << e.description()
				<< "\nExiting.\n";
	}

	return 0;
}
