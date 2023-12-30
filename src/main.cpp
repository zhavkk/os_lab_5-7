#include <iostream>
#include <unistd.h>
#include <string>
#include <vector>
#include <sstream>
#include <signal.h>
#include <cassert>
#include "../include/tree.h"
#include <zmq.hpp>
#include <chrono>
#include <thread>

using namespace std;

// const int TIMER = 500;
const int DEFAULT_PORT  = 5050;
int n = 2;

pthread_mutex_t mutex1; 
zmq::context_t context(1);
zmq::socket_t main_socket(context, ZMQ_REQ);


// общая функция для отправки сообщения в дочерний процесс
bool send_message(zmq::socket_t &socket, const string &message_string) {
    zmq::message_t message(message_string.size());
    memcpy(message.data(), message_string.c_str(), message_string.size());
    return socket.send(message); 
}


string receive_message(zmq::socket_t &socket) {
    zmq::message_t message;
    bool ok = false;
    try {
        ok = socket.recv(&message);
    }
    catch (...) {
        ok = false;
    }
    string recieved_message(static_cast<char*>(message.data()), message.size());
    if (recieved_message.empty() || !ok) {
        return "Root is dead";
    }
    return recieved_message;
}

// меняем созданый fork процесс на дочерний, передавая туда нужные нам аргументы 
void create_node(int id, int port) {
    char* arg0 = strdup("./client");
    char* arg1 = strdup((to_string(id)).c_str());
    char* arg2 = strdup((to_string(port)).c_str());
    char* args[] = {arg0, arg1, arg2, NULL};
    execv("./client", args);
}

// функция, собирающая полный адрес до дочернего процесса
string get_port_name(const int port) {
    return "tcp://127.0.0.1:" + to_string(port);
}

bool is_number(const string& val) {
    try {
        int tmp = stoi(val);
        return true;
    }
    catch(exception& e) {
        cout << "Error: " << e.what() << "\n";
        return false;
    }
}

int main() {
    Tree T;
    std::vector<int> nodes;
    string command;
    int child_pid = 0;
    int child_id = 0;
    pthread_mutex_init(&mutex1, NULL);
    cout << "Commands:\n";
    cout << "1. create (id)\n";
    cout << "2. exec (id) (name, value)\n";
    cout << "3. kill (id)\n";
    cout << "4. pingall \n";
    cout << "6. exit\n" << endl;
    while (true) {
        cin >> command;
        if (command == "create") {
	        n++;
            size_t node_id = 0;
            string str = "";
            string result = "";
            cin >> str;
            if (!is_number(str)) {
                continue;
            }
            node_id = stoi(str); 
            if (child_pid == 0) { // если у сервера еще нет дочернего процесса
                main_socket.bind(get_port_name(DEFAULT_PORT + node_id));
                // main_socket.set(zmq::sockopt::rcvtimeo, n * TIMER); // меняем максимальное время ожидание ответа
		        // main_socket.set(zmq::sockopt::sndtimeo, n * TIMER); // меняем макс время отправки сообщения 
	        	child_pid = fork(); // создаем дочерний процесс, вызванный фукнцией create
                if (child_pid == -1) {
                    cout << "Unable to create first worker node\n";
                    child_pid = 0;
                    exit(1);
                } else if (child_pid == 0) { // внутри дочернего
                    create_node(node_id, DEFAULT_PORT + node_id); // создаем исполняющий узел с заданным user id
                } else { // внутри серверного узла
                    child_id = node_id;
                    // main_socket.set(zmq::sockopt::rcvtimeo, n * TIMER);
		            // main_socket.set(zmq::sockopt::sndtimeo, n * TIMER);
                    send_message(main_socket, "pid"); // отправляем в дочерний процесс запрос на pid 
                    result = receive_message(main_socket); // получаем ответ с pid'ом
                }
            } else { // если у сервера есть исполнители
		        // main_socket.set(zmq::sockopt::rcvtimeo, n * TIMER);
		        // main_socket.set(zmq::sockopt::sndtimeo, n * TIMER);
                string msg_s = "create " + to_string(node_id);
                send_message(main_socket, msg_s);
                result = receive_message(main_socket);
            }
            if (result.substr(0, 2) == "Ok") { // если все создалось то добавляем в дерево айди кентика
                T.push(node_id);
                nodes.push_back(node_id);
            }
            cout << result << "\n";
        } else if (command == "kill") {
            int node_id = 0;
            string str = "";
            cin >> str;
            if (!is_number(str)) {
                continue;
            }
            node_id = stoi(str);
            if (child_pid == 0) { // если нет исполнителей
                cout << "Error: Not found\n";
                continue;
            }
            if (node_id == child_id) { // предполагаем, что если убить процесс посередке, дальше сигнал никак не пройдет
                kill(child_pid, SIGTERM);
                kill(child_pid, SIGKILL);
                child_id = 0;
                child_pid = 0;
                T.kill(node_id);
                cout << "Ok\n";
                continue;
            }
            string message_string = "kill " + to_string(node_id);
            send_message(main_socket, message_string);
            string recieved_message;
	        recieved_message = receive_message(main_socket);
            if (recieved_message.substr(0, min<int>(recieved_message.size(), 2)) == "Ok") {
                T.kill(node_id);
            }
            cout << recieved_message << "\n";
        }
        else if (command == "exec") {
            string input_string;
            string id_str = "";
            string name = "";
	        string value = "0";
            int id = 0;
            getline(cin, input_string);
            istringstream iss(input_string);
            vector<std::string> words;
            std::string word;
            while (iss >> word) {
                words.push_back(word);
            }
            id_str = words[0];
            if (!is_number(id_str)) {
                continue;
            }
            id = stoi(id_str);
            name = words[1];
            if (words.size() == 2) {
                string message_string = "exec " + to_string(id) + " " + name + " " + "NOVALUE";
                send_message(main_socket, message_string);
                string recieved_message = receive_message(main_socket);
                cout << recieved_message << "\n";
            }

            if (words.size() == 3) {
                value = words[2];
                string message_string = "exec " + to_string(id) + " " + name + " " + value;
                send_message(main_socket, message_string);
                string recieved_message = receive_message(main_socket);
                cout << recieved_message << "\n";
            }

            
        }
	    else if (command == "ping") {
	        string id_str = "";
            int id = 0;
            cin >> id_str;
            if (!is_number(id_str)) {
                continue;
            }
            id = stoi(id_str);
            string message_string = "ping " + to_string(id);
            send_message(main_socket, message_string);
	        string recieved_message = receive_message(main_socket);
            cout << recieved_message << "\n";
	    }
        else if (command == "pingall") {
            string unreachable_nodes;
            for (int i = 0; i < nodes.size(); i++) {
                int id = nodes[i];
                string message_string = "ping " + to_string(id);
                send_message(main_socket, message_string);
                string response = receive_message(main_socket);

                if (response.substr(0, 2) != "Ok") {
                    unreachable_nodes += to_string(id) + ";";
                }
            }

            if (unreachable_nodes.empty()) {
                cout << "Ok: -1\n";  // Все узлы доступны
            } else {
                unreachable_nodes.pop_back();  // Удаляем последнюю точку с запятой
                cout << "Ok: " << unreachable_nodes << "\n";
            }
        }



            else if (command == "exit") {
                int n = system("killall client");
                break;
            }
    }
    return 0;
}
