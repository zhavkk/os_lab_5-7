#include <iostream>
#include <unistd.h>
#include <stdlib.h>
#include <string>
#include <sstream>
#include <exception>
#include <map>
#include <signal.h>
#include <zmq.hpp>


using namespace std;

const int TIMER = 500;
const int DEFAULT_PORT  = 5050;
int n = 2;
std::map<std::string, int> m;

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
        return "";
    }
    return recieved_message;
}

void create_node(int id, int port) {
    char* arg0 = strdup("./client");
    char* arg1 = strdup((to_string(id)).c_str());
    char* arg2 = strdup((to_string(port)).c_str());
    char* args[] = {arg0, arg1, arg2, NULL};
    execv("./client", args);
}

string get_port_name(const int port) {
    return "tcp://127.0.0.1:" + to_string(port);
}

void real_create(zmq::socket_t& parent_socket, zmq::socket_t& socket, int& create_id, int& id, int& pid) {
    cout << to_string(id);
    if (pid == -1) {
        send_message(parent_socket, "Error: Cannot fork");
        pid = 0;
    } 
    else if (pid == 0) {
        create_node(create_id, DEFAULT_PORT + create_id);
    } 
    else {  // отправляем pid в дочерний и в родительский узлы
        id = create_id;
        send_message(socket, "pid");
        send_message(parent_socket, receive_message(socket));
    }
}

void real_kill(zmq::socket_t& parent_socket, zmq::socket_t& socket,  int& delete_id, int& id, int& pid, string& request_string) {
    if (id == 0) {
        send_message(parent_socket, "Error: Not found");
    } 
    else if (id == delete_id) {
        send_message(socket, "kill_children");
        receive_message(socket);
        kill(pid, SIGTERM);
        kill(pid, SIGKILL);
        id = 0;
        pid = 0;
        send_message(parent_socket, "Ok");
    } 
    else {
        send_message(socket, request_string);
        send_message(parent_socket, receive_message(socket));
    }
}

void real_exec(zmq::socket_t& parent_socket, zmq::socket_t& socket,  int& id, int& pid, string& request_string) {
    if (pid == 0) {
        string receive_message = "Error:" + to_string(id);
        receive_message += ": Not found";
        send_message(parent_socket, receive_message);
    } 
    else {
        send_message(socket, request_string);
        string str = receive_message(socket);
        if (str == "") str = "Error: Node is unavailable";
        send_message(parent_socket, str);
    }
}

void real_ping(zmq::socket_t& parent_socket, zmq::socket_t& socket, int& id, int& pid, string& request_string) {
    if (pid == 0) {
        string receive_message = "Error:" + to_string(id);
        receive_message += ": Not found";
        send_message(parent_socket, receive_message);
    }
    else {
        send_message(socket, request_string);
        string str = receive_message(socket);
        if (str == "") str = "Ok: 0";
        send_message(parent_socket, str);
    }
}


void exec(istringstream& command_stream, zmq::socket_t& parent_socket, zmq::socket_t& left_socket, 
            zmq::socket_t& right_socket, int& left_pid, int& right_pid, int& id, string& request_string) {
    string name, value;
    int exec_id;
    command_stream >> exec_id;
    if (exec_id == id) {
        command_stream >> name;
	    command_stream >> value;
        string receive_message = "";
	    string answer = "";

        if (value == "NOVALUE") {
            receive_message = "Ok:" + to_string(id) + ":";
            if (m.contains(name)) {
                receive_message += to_string(m[name]);
            } else {
                receive_message += " '" + name + "' not found";
            }
        } else {
            m[name] = stoi(value);
            receive_message = "Ok:" + to_string(id);
        }
        send_message(parent_socket, receive_message);
    } else if (exec_id < id) {
        real_exec(parent_socket, left_socket, exec_id, left_pid, request_string);
    } else {
        real_exec(parent_socket, right_socket, exec_id, right_pid, request_string);
    }
}

void ping(istringstream& command_stream, zmq::socket_t& parent_socket, zmq::socket_t& left_socket,
            zmq::socket_t& right_socket, int& left_pid, int& right_pid, int& id, string& request_string) {
    int ping_id;
    string receive_message;
    command_stream >> ping_id;
    if (ping_id == id) {
        receive_message = "Ok: 1";
        send_message(parent_socket, receive_message);
    } else if (ping_id < id) {
        real_ping(parent_socket, left_socket, ping_id, left_pid, request_string);
    }
    else {
        real_ping(parent_socket, right_socket, ping_id, right_pid, request_string);
    }
}


void kill_children(zmq::socket_t& parent_socket, zmq::socket_t& left_socket,
            zmq::socket_t& right_socket, int& left_pid, int& right_pid) {
    if (left_pid == 0 && right_pid == 0) {
        send_message(parent_socket, "Ok");
    } else {
        if (left_pid != 0) {
            send_message(left_socket, "kill_children");
            receive_message(left_socket);
            kill(left_pid, SIGTERM);
            kill(left_pid, SIGKILL);
        }
        if (right_pid != 0) {
            send_message(right_socket, "kill_children");
            receive_message(right_socket);
            kill(right_pid, SIGTERM);
            kill(right_pid, SIGKILL);
        }
        send_message(parent_socket, "Ok");
    }
}

int main(int argc, char** argv) {
    // анализируем входные данные
    int id = stoi(argv[1]);
    int parent_port = stoi(argv[2]);
    zmq::context_t context(3);
    zmq::socket_t parent_socket(context, ZMQ_REP);
    parent_socket.connect(get_port_name(parent_port));
    parent_socket.set(zmq::sockopt::rcvtimeo, TIMER);
    parent_socket.set(zmq::sockopt::sndtimeo, TIMER);
    int left_pid = 0;
    int right_pid = 0;
    int left_id = 0;
    int right_id = 0;
    zmq::socket_t left_socket(context, ZMQ_REQ);
    zmq::socket_t right_socket(context, ZMQ_REQ);


    while(true) {
        string request_string = receive_message(parent_socket); // получаем запрос от родительского процесса
        istringstream command_stream(request_string);
        string command;
        command_stream >> command;
        if (command == "id") {
            string parent_string = "Ok:" + to_string(id);
            send_message(parent_socket, parent_string);
        } else if (command == "pid") {
            string parent_string = "Ok:" + to_string(getpid());
            send_message(parent_socket, parent_string);
        } else if (command == "create") {
            int create_id;
            command_stream >> create_id;
            if (create_id == id) { // если айди занят
                string message_string = "Error: Already exists";
                send_message(parent_socket, message_string);
            } else if (create_id < id) { // если поддерево левое 
                if (left_pid == 0) { // если в левом дереве еще нет исполнителей
                    left_socket.bind(get_port_name(DEFAULT_PORT + create_id));
                    left_socket.set(zmq::sockopt::rcvtimeo, n * TIMER);
	                left_socket.set(zmq::sockopt::sndtimeo, n * TIMER);
                    left_pid = fork();
                    real_create(parent_socket, left_socket, create_id, left_id, left_pid);
                } else { // если в левом дереве уже есть исполнитель
                    send_message(left_socket, request_string);
                    string str = receive_message(left_socket);
                    if (str == "") {
                        left_socket.bind(get_port_name(DEFAULT_PORT + create_id));
                        left_socket.set(zmq::sockopt::rcvtimeo, n * TIMER);
                        left_socket.set(zmq::sockopt::sndtimeo, n * TIMER);
                        left_pid = fork();
                        real_create(parent_socket, left_socket, create_id, left_id, left_pid);
                    } else {
                        send_message(parent_socket, str); // отправляем результат об успешном создании в родителя
                        n++;
                        left_socket.set(zmq::sockopt::rcvtimeo, n * TIMER);
                        left_socket.set(zmq::sockopt::sndtimeo, n * TIMER);
                    }
                }
            } else { // если поддерево правое
                if (right_pid == 0) {
                    right_socket.bind(get_port_name(DEFAULT_PORT + create_id));
                    right_socket.set(zmq::sockopt::rcvtimeo, n * TIMER);
	                right_socket.set(zmq::sockopt::sndtimeo, n * TIMER);
                    
                    right_pid = fork();
                    real_create(parent_socket, right_socket, create_id, right_id, right_pid);
                } else {
                    send_message(right_socket, request_string);
                    string str = receive_message(right_socket);
                    if (str == "") {
                        right_socket.bind(get_port_name(DEFAULT_PORT + create_id));
                        right_socket.set(zmq::sockopt::rcvtimeo, n * TIMER);
                        right_socket.set(zmq::sockopt::sndtimeo, n * TIMER);
                        right_pid = fork();
                        real_create(parent_socket, right_socket, create_id, right_id, right_pid);
                    } else {
                        send_message(parent_socket, str);
                        n++;
                        right_socket.set(zmq::sockopt::rcvtimeo, n * TIMER);
                        right_socket.set(zmq::sockopt::sndtimeo, n * TIMER);
                    }
                }
            }
        } else if (command == "kill") {
            int delete_id;
            command_stream >> delete_id;
            if (delete_id < id) {
                real_kill(parent_socket, left_socket, delete_id, left_id, left_pid, request_string);
            } else {
                real_kill(parent_socket, right_socket, delete_id, right_id, right_pid, request_string);
            }
        } else if (command == "exec") {
            exec(command_stream, parent_socket, left_socket, right_socket, left_pid, right_pid, id, request_string);
        } else if (command == "ping") {
	        ping(command_stream, parent_socket, left_socket, right_socket, left_pid, right_pid, id, request_string);
        } else if (command == "kill_children") {
            kill_children(parent_socket, left_socket, right_socket, left_pid, right_pid);
        }
        if (parent_port == 0) {
            break;
        }
    }
    return 0;
}
