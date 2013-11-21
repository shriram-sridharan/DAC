#include "ClientSocket.h"
#include "SocketException.h"
#include <iostream>
#include <string>
#include <cstdlib>

int main(int argc, int argv[]) {
	try {

		ClientSocket client_socket("localhost", 30000);

		std::string reply;
		client_socket >> reply;
		if(reply == "-1")
		{
			std::cout << "Connection closed by server\n" << std::endl;
			exit(-1);
		}

		std::string echoinput = "";
		while (echoinput != "-1") {

			std::cin >> echoinput;
			try {
				client_socket << echoinput;
				client_socket >> reply;
			} catch (SocketException&) {
				break;
			}

			std::cout << "We received this response from the server:\n\""
					<< reply << "\"\n";
			;

		}
	} catch (SocketException& e) {
		std::cout << "Exception was caught:" << e.description() << "\n";
	}

	return 0;
}
